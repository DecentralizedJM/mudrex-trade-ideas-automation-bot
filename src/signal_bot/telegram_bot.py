"""
Telegram Bot - Centralized signal bot with webhook support.

This bot:
1. Receives signals from admin in the signal channel
2. Handles user registration via DM
3. Broadcasts trades to all subscribers
4. Notifies users of execution results
"""

import asyncio
import logging
from typing import Optional

from telegram import Update, Bot
from telegram.ext import (
    Application,
    CommandHandler,
    ContextTypes,
    MessageHandler,
    ConversationHandler,
    filters,
)

from .signal_parser import (
    SignalParser,
    Signal,
    SignalUpdate,
    SignalClose,
    SignalParseError,
    format_signal_summary,
)
from .broadcaster import (
    SignalBroadcaster,
    TradeStatus,
    format_broadcast_summary,
    format_user_trade_notification,
)
from .database import Database
from .settings import Settings

logger = logging.getLogger(__name__)

# Conversation states for registration
AWAITING_API_KEY, AWAITING_API_SECRET, AWAITING_AMOUNT = range(3)


class SignalBot:
    """
    Centralized Telegram Signal Bot.
    
    - Admin posts signals in channel → executes for all subscribers
    - Users DM to register with their Mudrex API keys
    - All API keys encrypted at rest
    """
    
    def __init__(self, settings: Settings, database: Database):
        """
        Initialize the signal bot.
        
        Args:
            settings: Application settings
            database: Database instance
        """
        self.settings = settings
        self.db = database
        self.broadcaster = SignalBroadcaster(database)
        self.app: Optional[Application] = None
        self.bot: Optional[Bot] = None
        
        logger.info(f"SignalBot initialized - Admin: {settings.admin_telegram_id}")
    
    def _is_admin(self, user_id: int) -> bool:
        """Check if user is the admin."""
        return user_id == self.settings.admin_telegram_id
    
    def _is_signal_channel(self, chat_id: int) -> bool:
        """Check if message is from the signal channel."""
        return chat_id == self.settings.signal_channel_id
    
    # ==================== User Commands ====================
    
    async def start_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle /start command."""
        user = update.effective_user
        
        # Check if already registered
        subscriber = await self.db.get_subscriber(user.id)
        
        if subscriber and subscriber.is_active:
            await update.message.reply_text(
                f"👋 Welcome back, {user.first_name}!\n\n"
                f"You're already registered.\n\n"
                f"**Your Settings:**\n"
                f"💰 Trade Amount: {subscriber.trade_amount_usdt} USDT\n"
                f"⚡ Max Leverage: {subscriber.max_leverage}x\n"
                f"📊 Total Trades: {subscriber.total_trades}\n\n"
                f"**Commands:**\n"
                f"/status - View your settings\n"
                f"/setamount - Change trade amount\n"
                f"/setleverage - Change max leverage\n"
                f"/unregister - Stop receiving signals",
                parse_mode="Markdown"
            )
        else:
            await update.message.reply_text(
                f"🤖 **Mudrex TradeIdeas Bot**\n\n"
                f"Welcome, {user.first_name}!\n\n"
                f"I auto-execute trading signals on your Mudrex account.\n\n"
                f"**To get started:**\n"
                f"/register - Connect your Mudrex account\n\n"
                f"**You'll need:**\n"
                f"• Mudrex API Key\n"
                f"• Mudrex API Secret\n\n"
                f"🔒 Your API keys are encrypted and stored securely.",
                parse_mode="Markdown"
            )
    
    async def status_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle /status command."""
        user = update.effective_user
        subscriber = await self.db.get_subscriber(user.id)
        
        if not subscriber or not subscriber.is_active:
            await update.message.reply_text(
                "❌ You're not registered.\n\nUse /register to get started."
            )
            return
        
        await update.message.reply_text(
            f"📊 **Your Status**\n"
            f"━━━━━━━━━━━━━━━━━━━━\n"
            f"💰 Trade Amount: **{subscriber.trade_amount_usdt} USDT**\n"
            f"⚡ Max Leverage: **{subscriber.max_leverage}x**\n"
            f"📈 Total Trades: **{subscriber.total_trades}**\n"
            f"💵 Total PnL: **${subscriber.total_pnl:.2f}**\n"
            f"━━━━━━━━━━━━━━━━━━━━\n"
            f"✅ Status: Active",
            parse_mode="Markdown"
        )
    
    # ==================== Registration Flow ====================
    
    async def register_start(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Start registration - ask for API key."""
        if not self.settings.allow_registration:
            await update.message.reply_text(
                "❌ Registration is currently closed."
            )
            return ConversationHandler.END
        
        # Check if already registered
        subscriber = await self.db.get_subscriber(update.effective_user.id)
        if subscriber and subscriber.is_active:
            await update.message.reply_text(
                "⚠️ You're already registered!\n\n"
                "Use /unregister first if you want to re-register."
            )
            return ConversationHandler.END
        
        await update.message.reply_text(
            "� **Registration Step 1/3**\n\n"
            "Please send your **Mudrex API Key**.\n\n"
            "You can get this from:\n"
            "Mudrex → Settings → API Keys\n\n"
            "🔒 Your key will be encrypted.\n\n"
            "/cancel to abort",
            parse_mode="Markdown"
        )
        return AWAITING_API_KEY
    
    async def register_api_key(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Receive API key, ask for secret."""
        api_key = update.message.text.strip()
        
        # Basic validation
        if len(api_key) < 10:
            await update.message.reply_text(
                "❌ That doesn't look like a valid API key.\n"
                "Please try again or /cancel"
            )
            return AWAITING_API_KEY
        
        # Store temporarily
        context.user_data['api_key'] = api_key
        
        # Delete the message with the API key for security
        try:
            await update.message.delete()
        except:
            pass
        
        await update.message.reply_text(
            "✅ API Key received!\n\n"
            "🔐 **Registration Step 2/3**\n\n"
            "Now send your **Mudrex API Secret**.\n\n"
            "🔒 Your secret will be encrypted.\n\n"
            "/cancel to abort",
            parse_mode="Markdown"
        )
        return AWAITING_API_SECRET
    
    async def register_api_secret(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Receive API secret, ask for trade amount."""
        api_secret = update.message.text.strip()
        
        # Basic validation
        if len(api_secret) < 10:
            await update.message.reply_text(
                "❌ That doesn't look like a valid API secret.\n"
                "Please try again or /cancel"
            )
            return AWAITING_API_SECRET
        
        # Store temporarily
        context.user_data['api_secret'] = api_secret
        
        # Delete the message with the secret for security
        try:
            await update.message.delete()
        except:
            pass
        
        await update.message.reply_text(
            "✅ API Secret received!\n\n"
            "💰 **Registration Step 3/3**\n\n"
            "How much **USDT** do you want to trade per signal?\n\n"
            f"Default: {self.settings.default_trade_amount} USDT\n\n"
            "Send a number (e.g., `50` or `100`) or /skip for default\n\n"
            "/cancel to abort",
            parse_mode="Markdown"
        )
        return AWAITING_AMOUNT
    
    async def register_amount(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Receive trade amount, complete registration."""
        text = update.message.text.strip()
        
        # Parse amount
        try:
            amount = float(text)
            if amount < 1:
                raise ValueError("Too small")
            if amount > 10000:
                raise ValueError("Too large")
        except ValueError:
            await update.message.reply_text(
                "❌ Please enter a valid amount between 1 and 10000.\n"
                "Or use /skip for default."
            )
            return AWAITING_AMOUNT
        
        # Complete registration
        return await self._complete_registration(update, context, amount)
    
    async def register_skip_amount(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Skip amount, use default."""
        return await self._complete_registration(
            update, context, self.settings.default_trade_amount
        )
    
    async def _complete_registration(
        self,
        update: Update,
        context: ContextTypes.DEFAULT_TYPE,
        amount: float
    ):
        """Complete the registration process."""
        user = update.effective_user
        api_key = context.user_data.get('api_key')
        api_secret = context.user_data.get('api_secret')
        
        if not api_key or not api_secret:
            await update.message.reply_text(
                "❌ Registration failed. Please try again with /register"
            )
            return ConversationHandler.END
        
        # Validate API credentials by making a test call
        await update.message.reply_text("🔄 Validating your API credentials...")
        
        try:
            import asyncio
            from mudrex import MudrexClient
            
            def validate_api(secret: str):
                """Sync validation - runs in thread."""
                client = MudrexClient(api_secret=secret)
                return client.wallet.get_futures_balance()
            
            # Run in thread with 15 second timeout
            try:
                balance = await asyncio.wait_for(
                    asyncio.to_thread(validate_api, api_secret),
                    timeout=15.0
                )
            except asyncio.TimeoutError:
                await update.message.reply_text(
                    "❌ **Validation timed out!**\n\n"
                    "The API request took too long. Please check:\n"
                    "1. Your API secret is correct\n"
                    "2. Mudrex API is accessible\n\n"
                    "Try again with /register",
                    parse_mode="Markdown"
                )
                return ConversationHandler.END
            
            if balance is None:
                await update.message.reply_text(
                    "❌ **Invalid API credentials!**\n\n"
                    "Could not connect to Mudrex. Please check:\n"
                    "1. Your API secret is correct\n"
                    "2. API has Futures trading permission\n\n"
                    "Try again with /register",
                    parse_mode="Markdown"
                )
                return ConversationHandler.END
                
            logger.info(f"API validated for {user.id}: Balance = {balance.balance} USDT")
            
        except Exception as e:
            logger.error(f"API validation failed for {user.id}: {e}")
            # Don't use Markdown - error messages may contain special chars
            await update.message.reply_text(
                f"❌ API validation failed!\n\n"
                f"Error: {str(e)[:100]}\n\n"
                f"Please check your credentials and try /register again."
            )
            return ConversationHandler.END
        
        # Save to database (encrypted)
        try:
            subscriber = await self.db.add_subscriber(
                telegram_id=user.id,
                username=user.username,
                api_key=api_key,
                api_secret=api_secret,
                trade_amount_usdt=amount,
                max_leverage=self.settings.default_max_leverage,
            )
            
            # Clear temporary data
            context.user_data.clear()
            
            await update.message.reply_text(
                f"🎉 **Registration Complete!**\n"
                f"━━━━━━━━━━━━━━━━━━━━\n"
                f"💰 Trade Amount: **{amount} USDT**\n"
                f"⚡ Max Leverage: **{self.settings.default_max_leverage}x**\n"
                f"━━━━━━━━━━━━━━━━━━━━\n\n"
                f"You'll now receive trades automatically when signals are posted!\n\n"
                f"**Commands:**\n"
                f"/status - View your settings\n"
                f"/setamount - Change trade amount\n"
                f"/setleverage - Change max leverage\n"
                f"/unregister - Stop receiving signals",
                parse_mode="Markdown"
            )
            
            logger.info(f"New subscriber registered: {user.id} (@{user.username})")
            
        except Exception as e:
            logger.error(f"Registration failed: {e}")
            await update.message.reply_text(
                f"❌ Registration failed: {e}\n\nPlease try again with /register"
            )
        
        return ConversationHandler.END
    
    async def register_cancel(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Cancel registration."""
        context.user_data.clear()
        await update.message.reply_text("❌ Registration cancelled.")
        return ConversationHandler.END
    
    # ==================== Settings Commands ====================
    
    async def setamount_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle /setamount command."""
        user = update.effective_user
        subscriber = await self.db.get_subscriber(user.id)
        
        if not subscriber or not subscriber.is_active:
            await update.message.reply_text("❌ You're not registered. Use /register first.")
            return
        
        args = context.args
        if not args:
            await update.message.reply_text(
                f"💰 Current trade amount: **{subscriber.trade_amount_usdt} USDT**\n\n"
                f"Usage: `/setamount <amount>`\n"
                f"Example: `/setamount 100`",
                parse_mode="Markdown"
            )
            return
        
        try:
            amount = float(args[0])
            if amount < 1 or amount > 10000:
                raise ValueError("Out of range")
            
            await self.db.update_trade_amount(user.id, amount)
            await update.message.reply_text(
                f"✅ Trade amount updated to **{amount} USDT**",
                parse_mode="Markdown"
            )
        except ValueError:
            await update.message.reply_text("❌ Please enter a valid amount between 1 and 10000")
    
    async def setleverage_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle /setleverage command."""
        user = update.effective_user
        subscriber = await self.db.get_subscriber(user.id)
        
        if not subscriber or not subscriber.is_active:
            await update.message.reply_text("❌ You're not registered. Use /register first.")
            return
        
        args = context.args
        if not args:
            await update.message.reply_text(
                f"⚡ Current max leverage: **{subscriber.max_leverage}x**\n\n"
                f"Usage: `/setleverage <amount>`\n"
                f"Example: `/setleverage 10`",
                parse_mode="Markdown"
            )
            return
        
        try:
            leverage = int(args[0])
            if leverage < 1 or leverage > 125:
                raise ValueError("Out of range")
            
            await self.db.update_max_leverage(user.id, leverage)
            await update.message.reply_text(
                f"✅ Max leverage updated to **{leverage}x**",
                parse_mode="Markdown"
            )
        except ValueError:
            await update.message.reply_text("❌ Please enter a valid leverage between 1 and 125")
    
    async def unregister_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle /unregister command."""
        user = update.effective_user
        
        success = await self.db.deactivate_subscriber(user.id)
        
        if success:
            await update.message.reply_text(
                "✅ You've been unregistered.\n\n"
                "You will no longer receive trading signals.\n"
                "Use /register to sign up again."
            )
        else:
            await update.message.reply_text("❌ You're not registered.")
    
    # ==================== Admin Commands ====================
    
    async def admin_stats_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle /adminstats command (admin only)."""
        if not self._is_admin(update.effective_user.id):
            return
        
        stats = await self.db.get_stats()
        
        await update.message.reply_text(
            f"📊 **Admin Stats**\n"
            f"━━━━━━━━━━━━━━━━━━━━\n"
            f"👥 Total Subscribers: {stats['total_subscribers']}\n"
            f"✅ Active: {stats['active_subscribers']}\n"
            f"📈 Total Trades: {stats['total_trades']}\n"
            f"📡 Active Signals: {stats['active_signals']}\n"
            f"━━━━━━━━━━━━━━━━━━━━",
            parse_mode="Markdown"
        )
    
    # ==================== Signal Handling ====================
    
    async def handle_signal_message(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle messages that might be signals."""
        message = update.message or update.channel_post
        
        if not message or not message.text:
            return
        
        text = message.text.strip()
        chat_id = message.chat_id
        
        # Debug logging
        logger.debug(f"Received message from chat {chat_id}: {text[:50]}...")
        
        # Only process commands
        if not text.startswith('/'):
            return
        
        # Check source
        user_id = message.from_user.id if message.from_user else None
        is_signal_channel = self._is_signal_channel(chat_id)
        is_admin_dm = user_id and self._is_admin(user_id) and message.chat.type == "private"
        
        logger.info(f"Signal check - chat_id: {chat_id}, user_id: {user_id}, is_channel: {is_signal_channel}, is_admin_dm: {is_admin_dm}")
        
        # Accept signals from:
        # 1. Admin's DM
        # 2. The designated signal channel (regardless of from_user - channel posts may not have it)
        if not is_admin_dm and not is_signal_channel:
            logger.debug(f"Ignoring message - not from admin DM or signal channel")
            return
        
        try:
            parsed = SignalParser.parse(text)
            
            if parsed is None:
                return
            
            logger.info(f"Parsed signal: {type(parsed).__name__}")
            
            if isinstance(parsed, Signal):
                await self._handle_new_signal(message, parsed)
            elif isinstance(parsed, SignalClose):
                await self._handle_close_signal(message, parsed)
                
        except SignalParseError as e:
            await message.reply_text(f"⚠️ Signal parse error: {e}")
        except Exception as e:
            logger.exception(f"Error handling signal: {e}")
    
    async def _handle_new_signal(self, message, signal: Signal):
        """Handle a new trading signal from admin."""
        # Show signal received
        summary = format_signal_summary(signal)
        await message.reply_text(summary, parse_mode="Markdown")
        
        # Broadcast to all subscribers
        results = await self.broadcaster.broadcast_signal(signal)
        
        # Send summary to admin
        broadcast_summary = format_broadcast_summary(signal, results)
        await message.reply_text(broadcast_summary, parse_mode="Markdown")
        
        # Notify each subscriber via DM
        for result in results:
            try:
                notification = format_user_trade_notification(signal, result)
                await self.bot.send_message(
                    chat_id=result.subscriber_id,
                    text=notification,
                    parse_mode="Markdown"
                )
            except Exception as e:
                logger.error(f"Failed to notify {result.subscriber_id}: {e}")
    
    async def _handle_close_signal(self, message, close: SignalClose):
        """Handle a close signal from admin."""
        await self.broadcaster.broadcast_close(close)
        await message.reply_text(
            f"✅ Signal `{close.signal_id}` marked as closed.\n\n"
            f"Subscribers have been notified.",
            parse_mode="Markdown"
        )
    
    # ==================== Bot Setup ====================
    
    async def _post_init(self, application: Application):
        """Called after Application.initialize() - connect database."""
        logger.info("Initializing database connection...")
        await self.db.connect()
        logger.info("Database connected successfully")
    
    async def _post_shutdown(self, application: Application):
        """Called after Application.shutdown() - close database."""
        logger.info("Closing database connection...")
        await self.db.close()
        logger.info("Database closed")
    
    def build_application(self) -> Application:
        """Build the Telegram application."""
        self.app = (
            Application.builder()
            .token(self.settings.telegram_bot_token)
            .post_init(self._post_init)
            .post_shutdown(self._post_shutdown)
            .build()
        )
        self.bot = self.app.bot
        
        # Registration conversation handler
        registration_handler = ConversationHandler(
            entry_points=[CommandHandler("register", self.register_start)],
            states={
                AWAITING_API_KEY: [
                    MessageHandler(filters.TEXT & ~filters.COMMAND, self.register_api_key)
                ],
                AWAITING_API_SECRET: [
                    MessageHandler(filters.TEXT & ~filters.COMMAND, self.register_api_secret)
                ],
                AWAITING_AMOUNT: [
                    CommandHandler("skip", self.register_skip_amount),
                    MessageHandler(filters.TEXT & ~filters.COMMAND, self.register_amount),
                ],
            },
            fallbacks=[CommandHandler("cancel", self.register_cancel)],
        )
        
        # Add handlers
        self.app.add_handler(CommandHandler("start", self.start_command))
        self.app.add_handler(CommandHandler("status", self.status_command))
        self.app.add_handler(registration_handler)
        self.app.add_handler(CommandHandler("setamount", self.setamount_command))
        self.app.add_handler(CommandHandler("setleverage", self.setleverage_command))
        self.app.add_handler(CommandHandler("unregister", self.unregister_command))
        self.app.add_handler(CommandHandler("adminstats", self.admin_stats_command))
        
        # Signal handlers - for both private messages and channel posts
        self.app.add_handler(MessageHandler(
            filters.TEXT & (filters.ChatType.PRIVATE | filters.ChatType.CHANNEL),
            self.handle_signal_message
        ))
        
        return self.app
    
    async def setup_webhook(self):
        """Set up webhook for Telegram."""
        webhook_url = self.settings.full_webhook_url
        
        if webhook_url:
            await self.bot.set_webhook(url=webhook_url)
            logger.info(f"Webhook set: {webhook_url}")
        else:
            logger.warning("No webhook URL configured, will use polling")
    
    def run_polling(self):
        """Run bot with polling (for local development)."""
        logger.info("Starting bot in polling mode...")
        self.build_application()
        self.app.run_polling(allowed_updates=Update.ALL_TYPES)

