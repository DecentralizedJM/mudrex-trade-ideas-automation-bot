"""
Signal Broadcaster - Execute trades for all subscribers when a signal is received.

This is the core of the centralized system:
1. Receive signal from admin
2. Loop through all active subscribers
3. Execute trade on each subscriber's Mudrex account (using SDK)
4. Notify each subscriber of result via Telegram DM
"""

import asyncio
import logging
from dataclasses import dataclass
from decimal import Decimal, ROUND_DOWN
from enum import Enum
from typing import List, Optional, Tuple

from mudrex import MudrexClient
from mudrex.exceptions import MudrexAPIError

from .database import Database, Subscriber
from .signal_parser import Signal, SignalType, OrderType, SignalUpdate, SignalClose

logger = logging.getLogger(__name__)


class TradeStatus(Enum):
    SUCCESS = "SUCCESS"
    INSUFFICIENT_BALANCE = "INSUFFICIENT_BALANCE"
    SYMBOL_NOT_FOUND = "SYMBOL_NOT_FOUND"
    API_ERROR = "API_ERROR"
    SKIPPED = "SKIPPED"


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


def calculate_quantity_from_usd(
    usd_amount: float,
    price: float,
    quantity_step: float,
    min_quantity: float = 0.0,
    max_quantity: float = float('inf'),
) -> Tuple[str, float]:
    """
    Calculate coin quantity from USD amount.
    
    Args:
        usd_amount: Amount in USD to trade
        price: Current price of the asset
        quantity_step: Minimum quantity increment (e.g., 0.001)
        min_quantity: Minimum allowed quantity
        max_quantity: Maximum allowed quantity
        
    Returns:
        Tuple of (quantity as string, actual USD value)
    """
    if price <= 0:
        return "0", 0.0
    
    # Calculate raw quantity
    raw_qty = usd_amount / price
    
    # Round down to quantity step
    if quantity_step > 0:
        step = Decimal(str(quantity_step))
        qty = Decimal(str(raw_qty)).quantize(step, rounding=ROUND_DOWN)
    else:
        qty = Decimal(str(raw_qty))
    
    # Apply min/max bounds
    qty = max(Decimal(str(min_quantity)), qty)
    qty = min(Decimal(str(max_quantity)), qty)
    
    # Calculate actual USD value
    actual_value = float(qty) * price
    
    # Format quantity string (remove trailing zeros)
    qty_str = f"{qty:f}".rstrip('0').rstrip('.')
    
    return qty_str, actual_value


class SignalBroadcaster:
    """
    Broadcast signals to all subscribers.
    
    Executes trades in parallel for all active subscribers using the Mudrex SDK.
    """
    
    def __init__(self, database: Database):
        self.db = database
    
    async def broadcast_signal(self, signal: Signal) -> List[TradeResult]:
        """
        Execute a signal for all active subscribers.
        
        Args:
            signal: The parsed trading signal
            
        Returns:
            List of trade results for each subscriber
        """
        logger.info(f"Broadcasting signal {signal.signal_id} to all subscribers")
        
        # Get all active subscribers
        subscribers = await self.db.get_active_subscribers()
        
        if not subscribers:
            logger.warning("No active subscribers to broadcast to")
            return []
        
        logger.info(f"Executing for {len(subscribers)} subscribers")
        
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
        
        # Execute for all subscribers in parallel
        tasks = [
            self._execute_for_subscriber(signal, subscriber)
            for subscriber in subscribers
        ]
        
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        # Filter out exceptions and log them
        trade_results = []
        for i, result in enumerate(results):
            if isinstance(result, Exception):
                logger.error(f"Trade failed for subscriber: {result}")
                trade_results.append(TradeResult(
                    subscriber_id=subscribers[i].telegram_id,
                    username=subscribers[i].username,
                    status=TradeStatus.API_ERROR,
                    message=str(result),
                ))
            else:
                trade_results.append(result)
        
        # Log summary
        success_count = sum(1 for r in trade_results if r.status == TradeStatus.SUCCESS)
        logger.info(f"Signal {signal.signal_id}: {success_count}/{len(trade_results)} successful")
        
        return trade_results
    
    async def _execute_for_subscriber(
        self,
        signal: Signal,
        subscriber: Subscriber,
    ) -> TradeResult:
        """Execute a signal for a single subscriber using the Mudrex SDK."""
        
        # Create SDK client for this subscriber
        client = MudrexClient(
            api_key=subscriber.api_key,
            api_secret=subscriber.api_secret
        )
        
        try:
            # FIXED: Use get_futures_balance(), not get()
            balance_info = client.wallet.get_futures_balance()
            balance = float(balance_info.balance) if balance_info else 0.0
            
            if balance < subscriber.trade_amount_usdt:
                await self.db.record_trade(
                    telegram_id=subscriber.telegram_id,
                    signal_id=signal.signal_id,
                    symbol=signal.symbol,
                    side=signal.signal_type.value,
                    order_type=signal.order_type.value,
                    status="INSUFFICIENT_BALANCE",
                    error_message=f"Balance: {balance:.2f} USDT",
                )
                return TradeResult(
                    subscriber_id=subscriber.telegram_id,
                    username=subscriber.username,
                    status=TradeStatus.INSUFFICIENT_BALANCE,
                    message=f"Insufficient balance: {balance:.2f} USDT",
                )
            
            # Get asset details for quantity calculation
            asset = client.assets.get(signal.symbol)
            if not asset:
                await self.db.record_trade(
                    telegram_id=subscriber.telegram_id,
                    signal_id=signal.signal_id,
                    symbol=signal.symbol,
                    side=signal.signal_type.value,
                    order_type=signal.order_type.value,
                    status="SYMBOL_NOT_FOUND",
                    error_message=f"Symbol {signal.symbol} not found",
                )
                return TradeResult(
                    subscriber_id=subscriber.telegram_id,
                    username=subscriber.username,
                    status=TradeStatus.SYMBOL_NOT_FOUND,
                    message=f"Symbol not found: {signal.symbol}",
                )
            
            # Set leverage (capped at subscriber's max)
            leverage = min(signal.leverage, subscriber.max_leverage)
            # FIXED: leverage must be string, include margin_type
            client.leverage.set(
                symbol=signal.symbol,
                leverage=str(leverage),
                margin_type="ISOLATED"
            )
            
            # FIXED: Calculate proper coin quantity from USD amount
            price = signal.entry_price if signal.entry_price else 1.0
            qty, actual_value = calculate_quantity_from_usd(
                usd_amount=subscriber.trade_amount_usdt,
                price=price,
                quantity_step=float(asset.quantity_step),
                min_quantity=float(asset.min_quantity),
                max_quantity=float(asset.max_quantity),
            )
            
            if qty == "0" or float(qty) < float(asset.min_quantity):
                await self.db.record_trade(
                    telegram_id=subscriber.telegram_id,
                    signal_id=signal.signal_id,
                    symbol=signal.symbol,
                    side=signal.signal_type.value,
                    order_type=signal.order_type.value,
                    status="API_ERROR",
                    error_message=f"Quantity {qty} below minimum {asset.min_quantity}",
                )
                return TradeResult(
                    subscriber_id=subscriber.telegram_id,
                    username=subscriber.username,
                    status=TradeStatus.API_ERROR,
                    message=f"Quantity too small: {qty} < {asset.min_quantity}",
                )
            
            # Determine side (SDK uses LONG/SHORT)
            side = "LONG" if signal.signal_type == SignalType.LONG else "SHORT"
            
            # Create order using SDK with proper quantity
            if signal.order_type == OrderType.MARKET:
                # Market order
                order = client.orders.create_market_order(
                    symbol=signal.symbol,
                    side=side,
                    quantity=qty,
                    leverage=str(leverage),
                )
            else:
                # Limit order
                order = client.orders.create_limit_order(
                    symbol=signal.symbol,
                    side=side,
                    price=str(signal.entry_price),
                    quantity=qty,
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
            msg = f"{side} {qty} {signal.symbol} (~${actual_value:.2f})"
            if signal.stop_loss or signal.take_profit:
                if sl_tp_set:
                    msg += " | SL/TP set ✓"
                else:
                    msg += f" | SL/TP failed: {sl_tp_error}" if sl_tp_error else " | No position for SL/TP"
            
            # Record success
            await self.db.record_trade(
                telegram_id=subscriber.telegram_id,
                signal_id=signal.signal_id,
                symbol=signal.symbol,
                side=side,
                order_type=signal.order_type.value,
                status="SUCCESS",
                quantity=float(qty),
                entry_price=signal.entry_price,
            )
            
            return TradeResult(
                subscriber_id=subscriber.telegram_id,
                username=subscriber.username,
                status=TradeStatus.SUCCESS,
                message=msg,
                order_id=order.order_id,
                quantity=qty,
                actual_value=actual_value,
            )
            
        except MudrexAPIError as e:
            logger.error(f"Mudrex API error for {subscriber.telegram_id}: {e}")
            await self.db.record_trade(
                telegram_id=subscriber.telegram_id,
                signal_id=signal.signal_id,
                symbol=signal.symbol,
                side=signal.signal_type.value,
                order_type=signal.order_type.value,
                status="API_ERROR",
                error_message=str(e),
            )
            return TradeResult(
                subscriber_id=subscriber.telegram_id,
                username=subscriber.username,
                status=TradeStatus.API_ERROR,
                message=f"API error: {e}",
            )
        except Exception as e:
            logger.error(f"Unexpected error for {subscriber.telegram_id}: {e}")
            await self.db.record_trade(
                telegram_id=subscriber.telegram_id,
                signal_id=signal.signal_id,
                symbol=signal.symbol,
                side=signal.signal_type.value,
                order_type=signal.order_type.value,
                status="API_ERROR",
                error_message=str(e),
            )
            return TradeResult(
                subscriber_id=subscriber.telegram_id,
                username=subscriber.username,
                status=TradeStatus.API_ERROR,
                message=f"Error: {e}",
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


def format_broadcast_summary(signal: Signal, results: List[TradeResult]) -> str:
    """Format broadcast results for admin notification."""
    success = sum(1 for r in results if r.status == TradeStatus.SUCCESS)
    failed = sum(1 for r in results if r.status == TradeStatus.API_ERROR)
    insufficient = sum(1 for r in results if r.status == TradeStatus.INSUFFICIENT_BALANCE)
    
    return f"""
📡 **Signal Broadcast Complete**
━━━━━━━━━━━━━━━━━━━━
🆔 Signal: `{signal.signal_id}`
📊 {signal.signal_type.value} {signal.symbol}

**Results:**
✅ Success: {success}
💰 Insufficient Balance: {insufficient}
❌ Failed: {failed}
━━━━━━━━━━━━━━━━━━━━
Total: {len(results)} subscribers
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
        return f"""
❌ **Trade Failed**
━━━━━━━━━━━━━━━━━━━━
🆔 Signal: `{signal.signal_id}`
📊 {signal.signal_type.value} {signal.symbol}

Error: {result.message}
━━━━━━━━━━━━━━━━━━━━
""".strip()
