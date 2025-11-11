#!/usr/bin/env python3
"""
High-Speed Wasabi Storage Telegram Bot
24/7 Operational with 4GB File Support
Direct Streaming - No Temporary Files
"""

import asyncio
import signal
import sys
from telegram_bot import TelegramWasabiBot
from config import config

class BotManager:
    def __init__(self):
        self.bot = None
        self.is_running = True
    
    def signal_handler(self, signum, frame):
        """Handle shutdown signals"""
        print(f"\n🛑 Received signal {signum}, shutting down gracefully...")
        self.is_running = False
        sys.exit(0)
    
    def setup_signals(self):
        """Setup signal handlers for graceful shutdown"""
        signal.signal(signal.SIGINT, self.signal_handler)
        signal.signal(signal.SIGTERM, self.signal_handler)
    
    async def start_bot(self):
        """Start the Telegram bot"""
        try:
            # Validate required environment variables
            required_vars = [
                'API_ID', 'API_HASH', 'BOT_TOKEN',
                'WASABI_ACCESS_KEY', 'WASABI_SECRET_KEY', 
                'WASABI_BUCKET', 'WASABI_REGION'
            ]
            
            for var in required_vars:
                if not getattr(config, var, None):
                    raise ValueError(f"Missing required environment variable: {var}")
            
            print("🔧 Initializing Wasabi Telegram Bot...")
            print("⚡ Mode: Direct Streaming (No Temporary Files)")
            
            # Initialize bot properly
            self.bot = TelegramWasabiBot()
            await self.bot.initialize()  # Fixed: await the initialization
            await self.bot.run()
            
        except Exception as e:
            print(f"❌ Failed to start bot: {e}")
            sys.exit(1)

async def main():
    """Main application entry point"""
    manager = BotManager()
    manager.setup_signals()
    
    # Start the bot
    await manager.start_bot()

if __name__ == "__main__":
    # Run the bot directly - no directory creation needed
    asyncio.run(main())
