"""
Trade Executor - Execute trades on Mudrex using the SDK.

Handles:
- Market and limit orders with proper quantity calculation
- Setting leverage (LONG/SHORT, not BUY/SELL)
- Stop loss and take profit (as separate risk orders)
- Balance checking via get_futures_balance()
"""

import logging
from dataclasses import dataclass
from decimal import Decimal, ROUND_DOWN
from enum import Enum
from typing import Optional, Tuple

from mudrex import MudrexClient
from mudrex.models import Order, Position, Asset
from mudrex.exceptions import MudrexAPIError

from .signal_parser import Signal, SignalType, OrderType, SignalUpdate, SignalClose

logger = logging.getLogger(__name__)


class ExecutionStatus(Enum):
    SUCCESS = "SUCCESS"
    INSUFFICIENT_BALANCE = "INSUFFICIENT_BALANCE"
    SYMBOL_NOT_FOUND = "SYMBOL_NOT_FOUND"
    LEVERAGE_ERROR = "LEVERAGE_ERROR"
    ORDER_FAILED = "ORDER_FAILED"
    POSITION_NOT_FOUND = "POSITION_NOT_FOUND"
    API_ERROR = "API_ERROR"


@dataclass
class ExecutionResult:
    """Result of trade execution."""
    status: ExecutionStatus
    message: str
    signal_id: str
    order: Optional[Order] = None
    position: Optional[Position] = None
    quantity: Optional[str] = None  # Actual quantity traded


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
        
    Example:
        >>> qty, value = calculate_quantity_from_usd(50.0, 1.905, 0.1)
        >>> # 50 / 1.905 = 26.24 → rounded to step 0.1 = 26.2
        >>> print(f"Qty: {qty}, Value: ${value:.2f}")
        Qty: 26.2, Value: $49.90
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


class TradeExecutor:
    """Execute trades on Mudrex using the SDK."""
    
    def __init__(
        self,
        api_key: str,
        api_secret: str,
        trade_amount_usdt: float = 50.0,
        max_leverage: int = 20,
        testnet: bool = False
    ):
        """
        Initialize the trade executor.
        
        Args:
            api_key: Mudrex API key
            api_secret: Mudrex API secret
            trade_amount_usdt: Amount in USDT to trade per signal
            max_leverage: Maximum allowed leverage
            testnet: Use testnet (not yet supported by Mudrex)
        """
        self.client = MudrexClient(api_key=api_key, api_secret=api_secret)
        self.trade_amount_usdt = trade_amount_usdt
        self.max_leverage = max_leverage
        self.testnet = testnet
        
        logger.info(f"TradeExecutor initialized - Amount: {trade_amount_usdt} USDT, Max Leverage: {max_leverage}x")
    
    def _check_balance(self) -> float:
        """Get available USDT futures balance."""
        try:
            # FIXED: Use get_futures_balance(), not get()
            balance = self.client.wallet.get_futures_balance()
            return float(balance.balance) if balance else 0.0
        except Exception as e:
            logger.error(f"Failed to get balance: {e}")
            return 0.0
    
    def _check_symbol_exists(self, symbol: str) -> bool:
        """Check if symbol exists on Mudrex."""
        try:
            return self.client.assets.exists(symbol)
        except Exception as e:
            logger.error(f"Failed to check symbol {symbol}: {e}")
            return False
    
    def _get_asset(self, symbol: str) -> Optional[Asset]:
        """Get asset details for quantity calculation."""
        try:
            return self.client.assets.get(symbol)
        except Exception as e:
            logger.error(f"Failed to get asset {symbol}: {e}")
            return None
    
    def _set_leverage(self, symbol: str, leverage: int) -> bool:
        """Set leverage for a symbol."""
        try:
            # Cap leverage at max allowed
            actual_leverage = min(leverage, self.max_leverage)
            
            # FIXED: leverage must be string, include margin_type
            self.client.leverage.set(
                symbol=symbol,
                leverage=str(actual_leverage),
                margin_type="ISOLATED"
            )
            logger.info(f"Set leverage for {symbol} to {actual_leverage}x")
            return True
        except Exception as e:
            logger.error(f"Failed to set leverage for {symbol}: {e}")
            return False
    
    def execute_signal(self, signal: Signal) -> ExecutionResult:
        """
        Execute a trading signal with proper quantity calculation.
        
        Args:
            signal: Parsed trading signal
            
        Returns:
            ExecutionResult with status and details
        """
        logger.info(f"Executing signal: {signal.signal_id} - {signal.signal_type.value} {signal.symbol}")
        
        # Step 1: Check balance
        balance = self._check_balance()
        if balance < self.trade_amount_usdt:
            msg = f"Insufficient balance: {balance:.2f} USDT available, need {self.trade_amount_usdt} USDT"
            logger.warning(msg)
            return ExecutionResult(
                status=ExecutionStatus.INSUFFICIENT_BALANCE,
                message=msg,
                signal_id=signal.signal_id
            )
        
        # Step 2: Get asset details for quantity calculation
        asset = self._get_asset(signal.symbol)
        if not asset:
            msg = f"Symbol not found: {signal.symbol}"
            logger.error(msg)
            return ExecutionResult(
                status=ExecutionStatus.SYMBOL_NOT_FOUND,
                message=msg,
                signal_id=signal.signal_id
            )
        
        # Step 3: Set leverage
        actual_leverage = min(signal.leverage, self.max_leverage)
        if not self._set_leverage(signal.symbol, actual_leverage):
            msg = f"Failed to set leverage to {actual_leverage}x for {signal.symbol}"
            logger.error(msg)
            return ExecutionResult(
                status=ExecutionStatus.LEVERAGE_ERROR,
                message=msg,
                signal_id=signal.signal_id
            )
        
        # Step 4: Calculate proper coin quantity from USD amount
        # For market orders, use entry_price as estimate (or we could fetch current price)
        price = signal.entry_price if signal.entry_price else 1.0
        
        qty, actual_value = calculate_quantity_from_usd(
            usd_amount=self.trade_amount_usdt,
            price=price,
            quantity_step=float(asset.quantity_step),
            min_quantity=float(asset.min_quantity),
            max_quantity=float(asset.max_quantity),
        )
        
        if qty == "0" or float(qty) < float(asset.min_quantity):
            msg = f"Calculated quantity {qty} below minimum {asset.min_quantity}"
            logger.error(msg)
            return ExecutionResult(
                status=ExecutionStatus.ORDER_FAILED,
                message=msg,
                signal_id=signal.signal_id
            )
        
        logger.info(f"Calculated: {qty} {signal.symbol} (~${actual_value:.2f} USDT)")
        
        # Step 5: Place order
        try:
            # FIXED: SDK uses LONG/SHORT, not BUY/SELL
            side = "LONG" if signal.signal_type == SignalType.LONG else "SHORT"
            
            if signal.order_type == OrderType.MARKET:
                # Market order
                order = self.client.orders.create_market_order(
                    symbol=signal.symbol,
                    side=side,
                    quantity=qty,
                    leverage=str(actual_leverage),
                )
            else:
                # Limit order
                order = self.client.orders.create_limit_order(
                    symbol=signal.symbol,
                    side=side,
                    quantity=qty,
                    price=str(signal.entry_price),
                    leverage=str(actual_leverage),
                )
            
            logger.info(f"Order placed successfully: {order}")
            
            # Set SL/TP after order is placed (more reliable than in initial order)
            sl_tp_set = False
            if order and (signal.stop_loss or signal.take_profit):
                try:
                    positions = self.client.positions.list_open()
                    position = next(
                        (p for p in positions if p.symbol == signal.symbol),
                        None
                    )
                    
                    if position:
                        self.client.positions.set_risk_order(
                            position_id=position.position_id,
                            stoploss_price=str(signal.stop_loss) if signal.stop_loss else None,
                            takeprofit_price=str(signal.take_profit) if signal.take_profit else None,
                        )
                        sl_tp_set = True
                        logger.info(f"Set SL/TP: SL={signal.stop_loss}, TP={signal.take_profit}")
                except Exception as e:
                    logger.warning(f"Failed to set SL/TP: {e}")
            
            # Build message
            msg = f"Order placed: {side} {qty} {signal.symbol} (~${actual_value:.2f})"
            if signal.stop_loss or signal.take_profit:
                msg += " | SL/TP set ✓" if sl_tp_set else " | SL/TP pending"
            
            return ExecutionResult(
                status=ExecutionStatus.SUCCESS,
                message=msg,
                signal_id=signal.signal_id,
                order=order,
                quantity=qty
            )
            
        except MudrexAPIError as e:
            msg = f"Order failed: {str(e)}"
            logger.error(msg)
            return ExecutionResult(
                status=ExecutionStatus.ORDER_FAILED,
                message=msg,
                signal_id=signal.signal_id
            )
        except Exception as e:
            msg = f"Unexpected error: {str(e)}"
            logger.error(msg)
            return ExecutionResult(
                status=ExecutionStatus.API_ERROR,
                message=msg,
                signal_id=signal.signal_id
            )
    
    def update_position(self, update: SignalUpdate, position_id: str) -> ExecutionResult:
        """
        Update an existing position's SL/TP using the positions API.
        
        Args:
            update: Signal update with new SL/TP values
            position_id: The position ID to update
            
        Returns:
            ExecutionResult with status
        """
        logger.info(f"Updating position for signal {update.signal_id}")
        
        try:
            # Get current position
            position = self.client.positions.get(position_id)
            if not position:
                return ExecutionResult(
                    status=ExecutionStatus.POSITION_NOT_FOUND,
                    message=f"Position not found for signal {update.signal_id}",
                    signal_id=update.signal_id
                )
            
            # FIXED: Use positions.set_risk_order() or edit_risk_order()
            sl_price = str(update.stop_loss) if update.stop_loss else None
            tp_price = str(update.take_profit) if update.take_profit else None
            
            success = self.client.positions.set_risk_order(
                position_id=position_id,
                stoploss_price=sl_price,
                takeprofit_price=tp_price,
            )
            
            if success:
                return ExecutionResult(
                    status=ExecutionStatus.SUCCESS,
                    message=f"Position updated: SL={update.stop_loss}, TP={update.take_profit}",
                    signal_id=update.signal_id,
                    position=position
                )
            else:
                return ExecutionResult(
                    status=ExecutionStatus.API_ERROR,
                    message="Failed to set risk order",
                    signal_id=update.signal_id
                )
            
        except MudrexAPIError as e:
            msg = f"Update failed: {str(e)}"
            logger.error(msg)
            return ExecutionResult(
                status=ExecutionStatus.API_ERROR,
                message=msg,
                signal_id=update.signal_id
            )
        except Exception as e:
            msg = f"Unexpected error: {str(e)}"
            logger.error(msg)
            return ExecutionResult(
                status=ExecutionStatus.API_ERROR,
                message=msg,
                signal_id=update.signal_id
            )
    
    def close_position(self, close: SignalClose, position_id: str) -> ExecutionResult:
        """
        Close a position using the SDK's positions.close() method.
        
        Args:
            close: Close signal
            position_id: The position ID to close
            
        Returns:
            ExecutionResult with status
        """
        logger.info(f"Closing position for signal {close.signal_id}")
        
        try:
            position = self.client.positions.get(position_id)
            if not position:
                return ExecutionResult(
                    status=ExecutionStatus.POSITION_NOT_FOUND,
                    message=f"Position not found for signal {close.signal_id}",
                    signal_id=close.signal_id
                )
            
            if close.partial_percent and close.partial_percent < 100:
                # FIXED: Use positions.close_partial() for partial closes
                close_qty = float(position.quantity) * (close.partial_percent / 100)
                
                # Round to quantity step if we have asset info
                asset = self._get_asset(position.symbol)
                if asset:
                    qty_str, _ = calculate_quantity_from_usd(
                        usd_amount=close_qty * 1000,  # Dummy high value
                        price=1000,  # Dummy price
                        quantity_step=float(asset.quantity_step),
                    )
                    # Actually we just need to round the close_qty
                    step = float(asset.quantity_step)
                    close_qty = int(close_qty / step) * step
                
                updated_position = self.client.positions.close_partial(
                    position_id=position_id,
                    quantity=str(close_qty)
                )
                
                return ExecutionResult(
                    status=ExecutionStatus.SUCCESS,
                    message=f"Partial close {close.partial_percent}% executed ({close_qty} closed)",
                    signal_id=close.signal_id,
                    position=updated_position
                )
            else:
                # FIXED: Use positions.close() for full close
                success = self.client.positions.close(position_id)
                
                if success:
                    return ExecutionResult(
                        status=ExecutionStatus.SUCCESS,
                        message=f"Position closed for signal {close.signal_id}",
                        signal_id=close.signal_id
                    )
                else:
                    return ExecutionResult(
                        status=ExecutionStatus.API_ERROR,
                        message="Failed to close position",
                        signal_id=close.signal_id
                    )
                
        except MudrexAPIError as e:
            msg = f"Close failed: {str(e)}"
            logger.error(msg)
            return ExecutionResult(
                status=ExecutionStatus.API_ERROR,
                message=msg,
                signal_id=close.signal_id
            )
        except Exception as e:
            msg = f"Unexpected error: {str(e)}"
            logger.error(msg)
            return ExecutionResult(
                status=ExecutionStatus.API_ERROR,
                message=msg,
                signal_id=close.signal_id
            )


def format_execution_result(result: ExecutionResult) -> str:
    """Format execution result for display."""
    
    status_emoji = {
        ExecutionStatus.SUCCESS: "✅",
        ExecutionStatus.INSUFFICIENT_BALANCE: "💰",
        ExecutionStatus.SYMBOL_NOT_FOUND: "❓",
        ExecutionStatus.LEVERAGE_ERROR: "⚠️",
        ExecutionStatus.ORDER_FAILED: "❌",
        ExecutionStatus.POSITION_NOT_FOUND: "🔍",
        ExecutionStatus.API_ERROR: "🚫",
    }
    
    emoji = status_emoji.get(result.status, "ℹ️")
    
    return f"""
{emoji} **Trade Execution**
━━━━━━━━━━━━━━━━━━━━
🆔 Signal: `{result.signal_id}`
📊 Status: {result.status.value}
💬 {result.message}
━━━━━━━━━━━━━━━━━━━━
""".strip()
