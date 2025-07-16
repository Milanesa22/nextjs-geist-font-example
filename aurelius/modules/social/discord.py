"""
AURELIUS Discord Integration Module
Handles Discord API integration for automated posting and bot interactions.
"""

import asyncio
import discord
from discord.ext import commands
import aiohttp
from typing import Dict, List, Optional, Any, Union
from datetime import datetime, timedelta
import json

from ...config import config
from ...logging_config import get_logger, log_social_activity, log_api_call
from ...utils.security import sanitize_for_social, validate_and_sanitize_input
from ...utils.rate_limit import rate_limiter, check_platform_rate_limit, increment_platform_usage
from ...db.redis_client import data_client

logger = get_logger("DISCORD")

class AureliusDiscord:
    """
    Discord API integration for automated posting, bot interactions, and webhook management.
    Supports both bot commands and webhook posting.
    """
    
    def __init__(self):
        self.bot_token = config.DISCORD_BOT_TOKEN
        self.webhook_url = config.DISCORD_WEBHOOK_URL
        self.channel_id = config.DISCORD_CHANNEL_ID
        
        # Discord bot client
        self.bot = None
        self.is_bot_running = False
        
        # HTTP client for webhook requests
        self.http_client = None
        
        # Track posted content and interactions
        self.posted_content_key = "discord:posted_content"
        self.messages_processed_key = "discord:messages_processed"
        self.interactions_key = "discord:interactions"
        
        self._initialize_bot()
    
    def _initialize_bot(self):
        """Initialize Discord bot with intents and event handlers."""
        try:
            # Set up intents
            intents = discord.Intents.default()
            intents.message_content = True
            intents.guilds = True
            intents.guild_messages = True
            intents.direct_messages = True
            
            # Create bot instance
            self.bot = commands.Bot(
                command_prefix='!aurelius ',
                intents=intents,
                help_command=None
            )
            
            # Set up event handlers
            self._setup_event_handlers()
            
            logger.info("✅ Discord bot initialized successfully")
            
        except Exception as e:
            logger.error(f"❌ Failed to initialize Discord bot: {e}")
            raise
    
    def _setup_event_handlers(self):
        """Set up Discord bot event handlers."""
        
        @self.bot.event
        async def on_ready():
            logger.info(f"🤖 Discord bot logged in as {self.bot.user}")
            self.is_bot_running = True
        
        @self.bot.event
        async def on_message(message):
            # Don't respond to own messages
            if message.author == self.bot.user:
                return
            
            # Process mentions and DMs
            if self.bot.user.mentioned_in(message) or isinstance(message.channel, discord.DMChannel):
                await self._handle_mention_or_dm(message)
            
            # Process commands
            await self.bot.process_commands(message)
        
        @self.bot.event
        async def on_error(event, *args, **kwargs):
            logger.error(f"❌ Discord bot error in {event}: {args}")
        
        # Add bot commands
        self._setup_bot_commands()
    
    def _setup_bot_commands(self):
        """Set up Discord bot commands."""
        
        @self.bot.command(name='help')
        async def help_command(ctx):
            """Show available commands."""
            embed = discord.Embed(
                title="AURELIUS Bot Commands",
                description="Available commands for AURELIUS AI assistant",
                color=0x00ff00
            )
            embed.add_field(
                name="!aurelius help",
                value="Show this help message",
                inline=False
            )
            embed.add_field(
                name="!aurelius status",
                value="Check bot status",
                inline=False
            )
            embed.add_field(
                name="!aurelius info",
                value="Get information about AURELIUS",
                inline=False
            )
            
            await ctx.send(embed=embed)
        
        @self.bot.command(name='status')
        async def status_command(ctx):
            """Check bot status."""
            embed = discord.Embed(
                title="AURELIUS Status",
                description="Bot is online and operational",
                color=0x00ff00
            )
            embed.add_field(
                name="Uptime",
                value=f"Since {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
                inline=True
            )
            embed.add_field(
                name="Servers",
                value=str(len(self.bot.guilds)),
                inline=True
            )
            
            await ctx.send(embed=embed)
        
        @self.bot.command(name='info')
        async def info_command(ctx):
            """Get information about AURELIUS."""
            embed = discord.Embed(
                title="About AURELIUS",
                description="Autonomous AI assistant for business management and social media automation",
                color=0x0099ff
            )
            embed.add_field(
                name="Features",
                value="• Automated social media posting\n• AI-powered content generation\n• Sales automation\n• Analytics and reporting",
                inline=False
            )
            
            await ctx.send(embed=embed)
    
    async def start_bot(self):
        """Start the Discord bot."""
        try:
            if not self.is_bot_running:
                logger.info("🚀 Starting Discord bot...")
                # Run bot in background task
                asyncio.create_task(self.bot.start(self.bot_token))
                
                # Wait for bot to be ready
                while not self.is_bot_running:
                    await asyncio.sleep(1)
                
                logger.info("✅ Discord bot started successfully")
            else:
                logger.info("ℹ️  Discord bot is already running")
                
        except Exception as e:
            logger.error(f"❌ Failed to start Discord bot: {e}")
            raise
    
    async def stop_bot(self):
        """Stop the Discord bot."""
        try:
            if self.is_bot_running:
                await self.bot.close()
                self.is_bot_running = False
                logger.info("🛑 Discord bot stopped")
            
        except Exception as e:
            logger.error(f"❌ Error stopping Discord bot: {e}")
    
    async def send_webhook_message(
        self,
        content: str,
        username: Optional[str] = "AURELIUS",
        avatar_url: Optional[str] = None,
        embeds: Optional[List[Dict[str, Any]]] = None
    ) -> Dict[str, Any]:
        """
        Send message via Discord webhook.
        Returns message data or error information.
        """
        try:
            if not self.webhook_url:
                return {
                    "success": False,
                    "error": "No webhook URL configured"
                }
            
            # Check rate limits
            if not await check_platform_rate_limit("discord", "post"):
                return {
                    "success": False,
                    "error": "Rate limit exceeded",
                    "retry_after": 3600
                }
            
            # Sanitize content
            sanitized_content = sanitize_for_social(content, "discord")
            if not sanitized_content:
                return {
                    "success": False,
                    "error": "Content failed sanitization"
                }
            
            # Check for duplicate content
            if await self._is_duplicate_content(sanitized_content):
                logger.warning("⚠️  Duplicate content detected, skipping post")
                return {
                    "success": False,
                    "error": "Duplicate content"
                }
            
            # Prepare webhook payload
            payload = {
                "content": sanitized_content,
                "username": username
            }
            
            if avatar_url:
                payload["avatar_url"] = avatar_url
            
            if embeds:
                payload["embeds"] = embeds
            
            # Initialize HTTP client if needed
            if not self.http_client:
                self.http_client = aiohttp.ClientSession()
            
            log_api_call("Discord", "POST webhook", "POST")
            
            # Send webhook request
            async with self.http_client.post(
                self.webhook_url,
                json=payload,
                headers={"Content-Type": "application/json"}
            ) as response:
                
                if response.status == 204:  # Discord webhook success
                    # Store posted content
                    await self._store_posted_content(sanitized_content, "webhook")
                    
                    # Increment usage counters
                    await increment_platform_usage("discord", "post")
                    
                    log_social_activity("Discord", "webhook_sent", sanitized_content[:50], success=True)
                    log_api_call("Discord", "POST webhook", "POST", status=204)
                    
                    result = {
                        "success": True,
                        "content": sanitized_content,
                        "username": username,
                        "timestamp": datetime.now().isoformat(),
                        "method": "webhook"
                    }
                    
                    logger.info("✅ Discord webhook message sent successfully")
                    return result
                    
                elif response.status == 429:  # Rate limited
                    retry_after = int(response.headers.get("Retry-After", 60))
                    error_msg = f"Discord webhook rate limited, retry after {retry_after}s"
                    logger.warning(f"⚠️  {error_msg}")
                    log_api_call("Discord", "POST webhook", "POST", status=429)
                    
                    return {
                        "success": False,
                        "error": error_msg,
                        "retry_after": retry_after
                    }
                else:
                    error_text = await response.text()
                    error_msg = f"Discord webhook failed with status {response.status}: {error_text}"
                    logger.error(f"❌ {error_msg}")
                    log_api_call("Discord", "POST webhook", "POST", status=response.status)
                    
                    return {
                        "success": False,
                        "error": error_msg
                    }
                    
        except Exception as e:
            error_msg = f"Failed to send Discord webhook: {e}"
            logger.error(f"❌ {error_msg}")
            log_social_activity("Discord", "webhook_sent", content[:50], success=False, error=error_msg)
            return {
                "success": False,
                "error": error_msg
            }
    
    async def send_channel_message(
        self,
        content: str,
        channel_id: Optional[str] = None,
        embed: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """
        Send message to Discord channel via bot.
        Returns message data or error information.
        """
        try:
            if not self.is_bot_running:
                return {
                    "success": False,
                    "error": "Discord bot is not running"
                }
            
            # Use provided channel ID or default
            target_channel_id = channel_id or self.channel_id
            if not target_channel_id:
                return {
                    "success": False,
                    "error": "No channel ID specified"
                }
            
            # Check rate limits
            if not await check_platform_rate_limit("discord", "post"):
                return {
                    "success": False,
                    "error": "Rate limit exceeded"
                }
            
            # Sanitize content
            sanitized_content = sanitize_for_social(content, "discord")
            if not sanitized_content:
                return {
                    "success": False,
                    "error": "Content failed sanitization"
                }
            
            # Get channel
            channel = self.bot.get_channel(int(target_channel_id))
            if not channel:
                return {
                    "success": False,
                    "error": f"Channel {target_channel_id} not found"
                }
            
            log_api_call("Discord", "POST /channels/:id/messages", "POST")
            
            # Send message
            if embed:
                discord_embed = discord.Embed.from_dict(embed)
                message = await channel.send(content=sanitized_content, embed=discord_embed)
            else:
                message = await channel.send(content=sanitized_content)
            
            # Store posted content
            await self._store_posted_content(sanitized_content, str(message.id))
            
            # Increment usage counters
            await increment_platform_usage("discord", "post")
            
            log_social_activity("Discord", "message_sent", sanitized_content[:50], success=True)
            log_api_call("Discord", "POST /channels/:id/messages", "POST", status=200)
            
            result = {
                "success": True,
                "message_id": str(message.id),
                "channel_id": str(channel.id),
                "content": sanitized_content,
                "timestamp": datetime.now().isoformat(),
                "method": "bot"
            }
            
            logger.info(f"✅ Discord message sent to channel {channel.name}")
            return result
            
        except discord.Forbidden:
            error_msg = "Bot lacks permission to send messages in this channel"
            logger.error(f"❌ {error_msg}")
            return {
                "success": False,
                "error": error_msg
            }
        except Exception as e:
            error_msg = f"Failed to send Discord channel message: {e}"
            logger.error(f"❌ {error_msg}")
            log_social_activity("Discord", "message_sent", content[:50], success=False, error=error_msg)
            return {
                "success": False,
                "error": error_msg
            }
    
    async def reply_to_message(self, message_id: str, channel_id: str, reply_content: str) -> Dict[str, Any]:
        """
        Reply to a specific Discord message.
        Returns reply data or error information.
        """
        try:
            if not self.is_bot_running:
                return {
                    "success": False,
                    "error": "Discord bot is not running"
                }
            
            # Get channel and message
            channel = self.bot.get_channel(int(channel_id))
            if not channel:
                return {
                    "success": False,
                    "error": f"Channel {channel_id} not found"
                }
            
            try:
                original_message = await channel.fetch_message(int(message_id))
            except discord.NotFound:
                return {
                    "success": False,
                    "error": f"Message {message_id} not found"
                }
            
            # Send reply
            result = await self.send_channel_message(
                content=reply_content,
                channel_id=channel_id
            )
            
            if result["success"]:
                logger.info(f"✅ Reply sent to message {message_id}")
                log_social_activity("Discord", "reply_sent", reply_content[:50], success=True)
            
            return result
            
        except Exception as e:
            error_msg = f"Failed to reply to Discord message {message_id}: {e}"
            logger.error(f"❌ {error_msg}")
            log_social_activity("Discord", "reply_sent", reply_content[:50], success=False, error=error_msg)
            return {
                "success": False,
                "error": error_msg
            }
    
    async def _handle_mention_or_dm(self, message):
        """Handle mentions and direct messages."""
        try:
            # Store interaction
            await self._store_interaction(message)
            
            # Generate response using AI (this would integrate with the AI module)
            # For now, we'll use a simple response
            if isinstance(message.channel, discord.DMChannel):
                response = "Hello! I'm AURELIUS, an AI assistant. How can I help you today?"
            else:
                response = f"Hello {message.author.mention}! I'm AURELIUS. How can I assist you?"
            
            # Send response
            await message.channel.send(response)
            
            log_social_activity("Discord", "mention_handled", message.content[:50], success=True)
            logger.info(f"✅ Handled mention/DM from {message.author}")
            
        except Exception as e:
            logger.error(f"❌ Error handling mention/DM: {e}")
    
    async def get_channel_messages(
        self,
        channel_id: str,
        limit: int = 50,
        before: Optional[str] = None,
        after: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        """
        Get messages from a Discord channel.
        Returns list of message data.
        """
        try:
            if not self.is_bot_running:
                logger.error("❌ Discord bot is not running")
                return []
            
            # Get channel
            channel = self.bot.get_channel(int(channel_id))
            if not channel:
                logger.error(f"❌ Channel {channel_id} not found")
                return []
            
            # Prepare parameters
            kwargs = {"limit": min(limit, 100)}
            
            if before:
                kwargs["before"] = discord.Object(id=int(before))
            if after:
                kwargs["after"] = discord.Object(id=int(after))
            
            log_api_call("Discord", "GET /channels/:id/messages", "GET")
            
            # Get messages
            messages = []
            async for message in channel.history(**kwargs):
                message_data = {
                    "id": str(message.id),
                    "content": message.content,
                    "author": {
                        "id": str(message.author.id),
                        "username": message.author.name,
                        "display_name": message.author.display_name,
                        "bot": message.author.bot
                    },
                    "timestamp": message.created_at.isoformat(),
                    "edited_timestamp": message.edited_at.isoformat() if message.edited_at else None,
                    "mentions": [str(user.id) for user in message.mentions],
                    "attachments": len(message.attachments),
                    "embeds": len(message.embeds),
                    "reactions": len(message.reactions)
                }
                messages.append(message_data)
            
            log_api_call("Discord", "GET /channels/:id/messages", "GET", status=200)
            logger.info(f"📬 Retrieved {len(messages)} messages from channel {channel_id}")
            
            return messages
            
        except Exception as e:
            error_msg = f"Failed to get channel messages: {e}"
            logger.error(f"❌ {error_msg}")
            return []
    
    async def _is_duplicate_content(self, content: str) -> bool:
        """Check if content has been posted recently."""
        try:
            recent_posts = await data_client.get(self.posted_content_key) or []
            
            content_lower = content.lower().strip()
            for post in recent_posts[-50:]:
                if post.get("content", "").lower().strip() == content_lower:
                    return True
            
            return False
            
        except Exception as e:
            logger.error(f"❌ Error checking duplicate content: {e}")
            return False
    
    async def _store_posted_content(self, content: str, message_id: str):
        """Store posted content to prevent duplicates."""
        try:
            recent_posts = await data_client.get(self.posted_content_key) or []
            
            post_data = {
                "content": content,
                "message_id": message_id,
                "timestamp": datetime.now().isoformat()
            }
            
            recent_posts.append(post_data)
            
            if len(recent_posts) > 100:
                recent_posts = recent_posts[-100:]
            
            await data_client.set(self.posted_content_key, recent_posts, expire=604800)
            
        except Exception as e:
            logger.error(f"❌ Error storing posted content: {e}")
    
    async def _store_interaction(self, message):
        """Store interaction data for analytics."""
        try:
            interactions = await data_client.get(self.interactions_key) or []
            
            interaction_data = {
                "message_id": str(message.id),
                "author_id": str(message.author.id),
                "author_name": message.author.name,
                "content": message.content[:200],  # Limit content length
                "channel_id": str(message.channel.id),
                "timestamp": datetime.now().isoformat(),
                "type": "dm" if isinstance(message.channel, discord.DMChannel) else "mention"
            }
            
            interactions.append(interaction_data)
            
            if len(interactions) > 500:
                interactions = interactions[-500:]
            
            await data_client.set(self.interactions_key, interactions, expire=2592000)  # 30 days
            
        except Exception as e:
            logger.error(f"❌ Error storing interaction: {e}")
    
    async def close(self):
        """Close HTTP client and stop bot."""
        try:
            if self.http_client:
                await self.http_client.close()
            
            await self.stop_bot()
            
        except Exception as e:
            logger.error(f"❌ Error closing Discord service: {e}")

# Global Discord service instance
discord_service = AureliusDiscord()

async def send_discord_webhook(content: str, **kwargs) -> Dict[str, Any]:
    """Quick function to send Discord webhook message."""
    return await discord_service.send_webhook_message(content, **kwargs)

async def send_discord_message(content: str, channel_id: Optional[str] = None, **kwargs) -> Dict[str, Any]:
    """Quick function to send Discord channel message."""
    return await discord_service.send_channel_message(content, channel_id, **kwargs)

async def start_discord_bot():
    """Quick function to start Discord bot."""
    await discord_service.start_bot()

async def stop_discord_bot():
    """Quick function to stop Discord bot."""
    await discord_service.stop_bot()

async def get_discord_messages(channel_id: str, limit: int = 50) -> List[Dict[str, Any]]:
    """Quick function to get Discord channel messages."""
    return await discord_service.get_channel_messages(channel_id, limit)
