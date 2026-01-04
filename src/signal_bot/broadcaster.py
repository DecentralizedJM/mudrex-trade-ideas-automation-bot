"""
Signal Broadcaster - Execute trades for all subscribers when a signal is received.

This is the core of the centralized system:
1. Receive signal from admin
2. Loop through all active subscribers
3. Execute trade on each subscriber's Mudrex account (using SDK) - IN PARALLEL
4. Notify each subscriber of result via Telegram DM
"""

import asyncio
import logging
from dataclasses import dataclass
from enum import Enum
from typing import List, Optional, Tuple

from mudrex import MudrexClient
from mudrex.exceptions import MudrexAPIError
from mudrex.utils import calculate_order_from_usd

from .database import Database, Subscriber
from .signal_parser import Signal, SignalType, OrderType, SignalUpdate, SignalClose

logger = logging.getLogger(__name__)


class TradeStatus(Enum):
    SUCCESS = "SUCCESS"
    INSUFFICIENT_BALANCE = "INSUFFICIENT_BALANCE"
    SYMBOL_NOT_FOUND = "SYMBOL_NOT_FOUND"
    API_ERROR = "API_ERROR"
    SKIPPED = "SKIPPED"
    PENDING_CONFIRMATION = "PENDING_CONFIRMATION"


@dataclass
class TradeResult:
    """Result of a trade execution for one subscriber."""
    subscriber_id: int
    username: Optional[str]
    status: TradeStatus
    message: str
    order_id: Optional[str] = None
    quantity: Optional[str] = None
    actual_value: Optional[float] = None
    # For DB recording
    side: Optional[str] = None
    order_type: Optional[str] = None
    entry_price: Optional[float] = None
    # For insufficient balance flow
    available_balance: Optional[float] = None


class SignalBroadcaster:
    """
    Broadcast signals to all subscribers.
    
    Executes trades in parallel for all active subscribers using the Mudrex SDK.
    """
    
    def __init__(self, database: Database):
        self.db = database
    
    async def broadcast_signal(self, signal: Signal) -> Tuple[List[TradeResult], List[Subscriber]]:
        """
        Execute a signal for all active subscribers.
        
        Args:
            signal: The parsed trading signal
            
        Returns:
            Tuple of:
            - List of trade results for AUTO mode subscribers
            - List of MANUAL mode subscribers (for confirmation flow)
        """
        logger.info(f"Broadcasting signal {signal.signal_id} to all subscribers")
        
        # Get all active subscribers
        subscribers = await self.db.get_active_subscribers()
        
        if not subscribers:
            logger.warning("No active subscribers to broadcast to")
            return [], []
        
        logger.info(f"Found {len(subscribers)} subscribers")
        
        # Separate AUTO and MANUAL subscribers
        auto_subscribers = [s for s in subscribers if s.trade_mode == "AUTO"]
        manual_subscribers = [s for s in subscribers if s.trade_mode == "MANUAL"]
        
        logger.info(f"AUTO: {len(auto_subscribers)}, MANUAL: {len(manual_subscribers)}")
        
        # Save signal to database
        await self.db.save_signal(
            signal_id=signal.signal_id,
            symbol=signal.symbol,
            signal_type=signal.signal_type.value,
            order_type=signal.order_type.value,
            entry_price=signal.entry_price,
            stop_loss=signal.stop_loss,
            take_profit=signal.take_profit,
            leverage=signal.leverage,
        )
        
        # Execute for AUTO subscribers in parallel
        if auto_subscribers:
            tasks = [
                self._execute_for_subscriber(signal, subscriber)
                for subscriber in auto_subscribers
            ]
            
            results = await asyncio.gather(*tasks, return_exceptions=True)
            
            # Filter out exceptions and log them
            trade_results = []
            for i, result in enumerate(results):
                if isinstance(result, Exception):
                    logger.error(f"Trade failed for subscriber: {result}")
                    trade_results.append(TradeResult(
                        subscriber_id=auto_subscribers[i].telegram_id,
                        username=auto_subscribers[i].username,
                        status=TradeStatus.API_ERROR,
                        message=str(result),
                    ))
                else:
                    trade_results.append(result)
            
            # Log summary
            success_count = sum(1 for r in trade_results if r.status == TradeStatus.SUCCESS)
            logger.info(f"Signal {signal.signal_id}: {success_count}/{len(trade_results)} AUTO trades successful")
        else:
            trade_results = []
        
        return trade_results, manual_subscribers
    
    async def _execute_for_subscriber(
        self,
        signal: Signal,
        subscriber: Subscriber,
    ) -> TradeResult:
        """Execute a signal for a single subscriber using the Mudrex SDK."""
        
        # Run the blocking SDK calls in a thread pool for true parallelism
        try:
            result = await asyncio.to_thread(
                self._execute_trade_sync,
                signal,
                subscriber,
            )
        except Exception as e:
            logger.error(f"Trade execution failed for {subscriber.telegram_id}: {e}")
            result = TradeResult(
                subscriber_id=subscriber.telegram_id,
                username=subscriber.username,
                status=TradeStatus.API_ERROR,
                message=str(e),
                side=signal.signal_type.value,
                order_type=signal.order_type.value,
            )
        
        # Record trade to database (async, after thread completes)
        await self.db.record_trade(
            telegram_id=subscriber.telegram_id,
            signal_id=signal.signal_id,
            symbol=signal.symbol,
            side=result.side or signal.signal_type.value,
            order_type=result.order_type or signal.order_type.value,
            status=result.status.value,
            quantity=float(result.quantity) if result.quantity else None,
            entry_price=result.entry_price,
            error_message=result.message if result.status != TradeStatus.SUCCESS else None,
        )
        
        return result
    
    def _execute_trade_sync(
        self,
        signal: Signal,
        subscriber: Subscriber,
    ) -> TradeResult:
        """
        Synchronous trade execution - runs in thread pool.
        This allows multiple trades to execute in parallel.
        """
        # Create SDK client for this subscriber (only api_secret needed)
        client = MudrexClient(
            api_secret=subscriber.api_secret
        )
        
        try:
            # FIXED: Use get_futures_balance(), not get()
            balance_info = client.wallet.get_futures_balance()
            balance = float(balance_info.balance) if balance_info else 0.0
            
            # Check if balance is sufficient - include available_balance for potential reduced trade
            if balance < subscriber.trade_amount_usdt:
                # Check if we have at least $1 to trade
                if balance < 1.0:
                    return TradeResult(
                        subscriber_id=subscriber.telegram_id,
                        username=subscriber.username,
                        status=TradeStatus.INSUFFICIENT_BALANCE,
                        message=f"Balance too low: {balance:.2f} USDT (min $1 required)",
                        side=signal.signal_type.value,
                        order_type=signal.order_type.value,
                        available_balance=balance,
                    )
                # We have some balance, return with available amount for user to decide
                return TradeResult(
                    subscriber_id=subscriber.telegram_id,
                    username=subscriber.username,
                    status=TradeStatus.INSUFFICIENT_BALANCE,
                    message=f"Requested: {subscriber.trade_amount_usdt} USDT, Available: {balance:.2f} USDT",
                    side=signal.signal_type.value,
                    order_type=signal.order_type.value,
                    available_balance=balance,
                )
            
            # Get asset details for quantity calculation
            asset = client.assets.get(signal.symbol)
            if not asset:
                return TradeResult(
                    subscriber_id=subscriber.telegram_id,
                    username=subscriber.username,
                    status=TradeStatus.SYMBOL_NOT_FOUND,
                    message=f"Symbol not found: {signal.symbol}",
                    side=signal.signal_type.value,
                    order_type=signal.order_type.value,
                )
            
            # Set leverage (capped at subscriber's max)
            leverage = min(signal.leverage, subscriber.max_leverage)
            # FIXED: leverage must be string, include margin_type
            client.leverage.set(
                symbol=signal.symbol,
                leverage=str(leverage),
                margin_type="ISOLATED"
            )
            
            # Calculate proper coin quantity from USD amount using SDK helper
            price = signal.entry_price if signal.entry_price else 1.0
            qty, actual_value = calculate_order_from_usd(
                usd_amount=subscriber.trade_amount_usdt,
                price=price,
                quantity_step=float(asset.quantity_step),
            )
            
            # Check minimum notional value (Mudrex requires ~$5 minimum)
            MIN_NOTIONAL_USDT = 5.0
            if actual_value < MIN_NOTIONAL_USDT:
                return TradeResult(
                    subscriber_id=subscriber.telegram_id,
                    username=subscriber.username,
                    status=TradeStatus.API_ERROR,
                    message=f"Trade amount ${subscriber.trade_amount_usdt:.2f} is below minimum ${MIN_NOTIONAL_USDT:.0f}. Use /setamount to increase.",
                    side=signal.signal_type.value,
                    order_type=signal.order_type.value,
                )
            
            # Validate against min/max
            min_qty = float(asset.min_quantity)
            max_qty = float(asset.max_quantity)
            if qty < min_qty:
                return TradeResult(
                    subscriber_id=subscriber.telegram_id,
                    username=subscriber.username,
                    status=TradeStatus.API_ERROR,
                    message=f"Quantity too small: {qty} < {asset.min_quantity}",
                    side=signal.signal_type.value,
                    order_type=signal.order_type.value,
                )
            
            # Determine side (SDK uses LONG/SHORT)
            side = "LONG" if signal.signal_type == SignalType.LONG else "SHORT"
            
            # Create order using SDK with proper quantity
            # Note: SDK now auto-rounds quantity, so we can pass it directly
            qty_str = str(qty)
            
            logger.info(f"Creating order: symbol={signal.symbol}, side={side}, qty={qty_str}, leverage={leverage}, order_type={signal.order_type.value}, entry_price={signal.entry_price}")
            
            if signal.order_type == OrderType.MARKET:
                # Market order
                order = client.orders.create_market_order(
                    symbol=signal.symbol,
                    side=side,
                    quantity=qty_str,
                    leverage=str(leverage),
                )
            else:
                # Limit order
                order = client.orders.create_limit_order(
                    symbol=signal.symbol,
                    side=side,
                    price=str(signal.entry_price),
                    quantity=qty_str,
                    leverage=str(leverage),
                )
            
            # Set SL/TP after order is placed (more reliable than in initial order)
            sl_tp_set = False
            sl_tp_error = None
            if order and (signal.stop_loss or signal.take_profit):
                try:
                    # Find the position for this order
                    positions = client.positions.list_open()
                    position = next(
                        (p for p in positions if p.symbol == signal.symbol),
                        None
                    )
                    
                    if position:
                        client.positions.set_risk_order(
                            position_id=position.position_id,
                            stoploss_price=str(signal.stop_loss) if signal.stop_loss else None,
                            takeprofit_price=str(signal.take_profit) if signal.take_profit else None,
                        )
                        sl_tp_set = True
                        logger.info(f"Set SL/TP for {subscriber.telegram_id}: SL={signal.stop_loss}, TP={signal.take_profit}")
                except Exception as e:
                    # Log but don't fail the trade - order was already placed successfully
                    sl_tp_error = str(e)
                    logger.warning(f"Failed to set SL/TP for {subscriber.telegram_id}: {e}")
            
            # Build success message
            msg = f"{side} {qty_str} {signal.symbol} (~${actual_value:.2f})"
            if signal.stop_loss or signal.take_profit:
                if sl_tp_set:
                    msg += " | SL/TP set ✓"
                else:
                    msg += f" | SL/TP failed: {sl_tp_error}" if sl_tp_error else " | No position for SL/TP"
            
            return TradeResult(
                subscriber_id=subscriber.telegram_id,
                username=subscriber.username,
                status=TradeStatus.SUCCESS,
                message=msg,
                order_id=order.order_id,
                quantity=qty_str,
                actual_value=actual_value,
                side=side,
                order_type=signal.order_type.value,
                entry_price=signal.entry_price,
            )
            
        except MudrexAPIError as e:
            logger.error(f"Mudrex API error for {subscriber.telegram_id}: {e}")
            return TradeResult(
                subscriber_id=subscriber.telegram_id,
                username=subscriber.username,
                status=TradeStatus.API_ERROR,
                message=f"API error: {e}",
                side=signal.signal_type.value,
                order_type=signal.order_type.value,
            )
        except Exception as e:
            logger.error(f"Unexpected error for {subscriber.telegram_id}: {e}")
            return TradeResult(
                subscriber_id=subscriber.telegram_id,
                username=subscriber.username,
                status=TradeStatus.API_ERROR,
                message=f"Error: {e}",
                side=signal.signal_type.value,
                order_type=signal.order_type.value,
            )
    
    async def broadcast_close(self, close: SignalClose) -> List[TradeResult]:
        """
        Broadcast a close signal to all subscribers.
        
        Note: This is more complex as we need to track which subscribers
        have open positions for this signal. For MVP, we'll mark the signal
        as closed and subscribers can manage manually.
        """
        logger.info(f"Broadcasting close for signal {close.signal_id}")
        
        await self.db.close_signal(close.signal_id)
        
        # For MVP, just mark as closed. Position closing would require
        # tracking position IDs per subscriber, which we can add later.
        return []
    
    async def execute_single_trade(self, signal: Signal, subscriber: Subscriber) -> TradeResult:
        """
        Execute a single trade for a specific subscriber.
        Used for manual confirmation flow.
        
        Args:
            signal: The parsed trading signal
            subscriber: The subscriber who confirmed the trade
            
        Returns:
            Trade result
        """
        logger.info(f"Executing confirmed trade for {subscriber.telegram_id}: {signal.signal_id}")
        return await self._execute_for_subscriber(signal, subscriber)
    
    async def execute_with_amount(
        self, 
        signal: Signal, 
        subscriber: Subscriber, 
        override_amount: float
    ) -> TradeResult:
        """
        Execute a trade with a specific override amount.
        Used when user accepts to trade with available balance instead of configured amount.
        
        Args:
            signal: The parsed trading signal
            subscriber: The subscriber
            override_amount: The amount to use instead of subscriber.trade_amount_usdt
            
        Returns:
            Trade result
        """
        logger.info(f"Executing trade for {subscriber.telegram_id} with override amount: {override_amount} USDT")
        
        # Create a modified subscriber with the override amount
        from dataclasses import replace
        modified_subscriber = replace(subscriber, trade_amount_usdt=override_amount)
        
        return await self._execute_for_subscriber(signal, modified_subscriber)


def format_broadcast_summary(signal: Signal, results: List[TradeResult], manual_count: int = 0) -> str:
    """Format broadcast results for admin notification."""
    success = sum(1 for r in results if r.status == TradeStatus.SUCCESS)
    failed = sum(1 for r in results if r.status == TradeStatus.API_ERROR)
    insufficient = sum(1 for r in results if r.status == TradeStatus.INSUFFICIENT_BALANCE)
    
    manual_line = f"\n👆 Manual (awaiting): {manual_count}" if manual_count > 0 else ""
    
    return f"""
📡 **Signal Broadcast Complete**
━━━━━━━━━━━━━━━━━━━━
🆔 Signal: `{signal.signal_id}`
📊 {signal.signal_type.value} {signal.symbol}

**Results:**
✅ Success: {success}
💰 Insufficient Balance: {insufficient}
❌ Failed: {failed}{manual_line}
━━━━━━━━━━━━━━━━━━━━
Total: {len(results) + manual_count} subscribers
""".strip()


def format_user_trade_notification(signal: Signal, result: TradeResult) -> str:
    """Format trade result notification for a subscriber."""
    if result.status == TradeStatus.SUCCESS:
        qty_info = f"\n📦 Quantity: {result.quantity}" if result.quantity else ""
        value_info = f" (~${result.actual_value:.2f})" if result.actual_value else ""
        return f"""
✅ **Trade Executed**
━━━━━━━━━━━━━━━━━━━━
🆔 Signal: `{signal.signal_id}`
📊 {signal.signal_type.value} {signal.symbol}
📋 {signal.order_type.value}{qty_info}{value_info}
🛑 SL: {signal.stop_loss or "Not set"}
🎯 TP: {signal.take_profit or "Not set"}
⚡ Leverage: {signal.leverage}x
━━━━━━━━━━━━━━━━━━━━
""".strip()
    
    elif result.status == TradeStatus.INSUFFICIENT_BALANCE:
        return f"""
💰 **Trade Skipped - Insufficient Balance**
━━━━━━━━━━━━━━━━━━━━
🆔 Signal: `{signal.signal_id}`
📊 {signal.signal_type.value} {signal.symbol}

{result.message}

Top up your Mudrex wallet to receive future signals.
━━━━━━━━━━━━━━━━━━━━
""".strip()
    
    else:
        # Escape special characters in error message for Markdown
        safe_message = result.message.replace('|', '\\|').replace('_', '\\_').replace('*', '\\*')
        return f"""
❌ **Trade Failed**
━━━━━━━━━━━━━━━━━━━━
🆔 Signal: `{signal.signal_id}`
📊 {signal.signal_type.value} {signal.symbol}

Error: {safe_message}
━━━━━━━━━━━━━━━━━━━━
""".strip()
