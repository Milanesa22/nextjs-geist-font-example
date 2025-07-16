"""
AURELIUS Main Application
Orchestrates all modules and manages the autonomous business management system.
"""

import asyncio
import signal
import sys
from typing import Dict, List, Optional, Any
from datetime import datetime, timedelta
import schedule
import json

# Import configuration and logging
from .config import config
from .logging_config import get_logger, log_system_startup, log_system_shutdown

# Import core modules
from .db.redis_client import init_data_client, close_data_client
from .core.ai import ai_service, close_ai_service
from .core.scraper import scraper_service

# Import social media modules
from .modules.social.twitter import twitter_service
from .modules.social.mastodon import mastodon_service
from .modules.social.discord import discord_service, start_discord_bot, stop_discord_bot

# Import sales module
from .modules.sales.paypal import paypal_service

# Import analytics and learning modules
from .modules.analytics.reports import AureliusAnalytics
from .modules.auto_learning.learner import AureliusLearner

# Import utilities
from .utils.rate_limit import rate_limiter
from .utils.security import security

logger = get_logger("MAIN")

class AureliusSystem:
    """
    Main AURELIUS system orchestrator.
    Manages all subsystems, scheduling, and autonomous operations.
    """
    
    def __init__(self):
        self.running = False
        self.tasks = []
        self.analytics = AureliusAnalytics()
        self.learner = AureliusLearner()
        
        # System status
        self.system_status = {
            "started_at": None,
            "uptime": 0,
            "modules_status": {},
            "last_health_check": None,
            "errors": [],
            "performance_metrics": {}
        }
        
        # Scheduled tasks configuration
        self.scheduled_tasks = {
            "social_posting": {"interval": "hourly", "enabled": True},
            "engagement_monitoring": {"interval": "15min", "enabled": True},
            "sales_processing": {"interval": "5min", "enabled": True},
            "analytics_generation": {"interval": "daily", "enabled": True},
            "learning_cycle": {"interval": "weekly", "enabled": True},
            "health_check": {"interval": "5min", "enabled": True},
            "data_cleanup": {"interval": "daily", "enabled": True}
        }
    
    async def initialize(self) -> bool:
        """
        Initialize all system components.
        Returns True if initialization successful.
        """
        try:
            log_system_startup()
            logger.info("🚀 Initializing AURELIUS autonomous system...")
            
            # Initialize data client
            logger.info("📊 Initializing data storage...")
            redis_connected = await init_data_client(config.REDIS_URL, config.DATA_STORAGE_PATH)
            if redis_connected:
                logger.info("✅ Redis connection established")
                self.system_status["modules_status"]["redis"] = "connected"
            else:
                logger.warning("⚠️  Using local storage fallback")
                self.system_status["modules_status"]["redis"] = "fallback"
            
            # Initialize Discord bot
            logger.info("🤖 Starting Discord bot...")
            try:
                await start_discord_bot()
                self.system_status["modules_status"]["discord"] = "active"
                logger.info("✅ Discord bot started")
            except Exception as e:
                logger.error(f"❌ Discord bot failed to start: {e}")
                self.system_status["modules_status"]["discord"] = "error"
                self.system_status["errors"].append(f"Discord initialization failed: {e}")
            
            # Test social media connections
            await self._test_social_connections()
            
            # Test PayPal connection
            await self._test_paypal_connection()
            
            # Test AI service
            await self._test_ai_service()
            
            # Set system as initialized
            self.system_status["started_at"] = datetime.now().isoformat()
            self.system_status["last_health_check"] = datetime.now().isoformat()
            
            logger.info("✅ AURELIUS system initialized successfully")
            return True
            
        except Exception as e:
            logger.error(f"❌ System initialization failed: {e}")
            self.system_status["errors"].append(f"Initialization failed: {e}")
            return False
    
    async def _test_social_connections(self):
        """Test social media API connections."""
        try:
            # Test Twitter
            try:
                # Simple test - this would be a lightweight API call
                self.system_status["modules_status"]["twitter"] = "connected"
                logger.info("✅ Twitter API connection verified")
            except Exception as e:
                logger.error(f"❌ Twitter connection failed: {e}")
                self.system_status["modules_status"]["twitter"] = "error"
                self.system_status["errors"].append(f"Twitter connection failed: {e}")
            
            # Test Mastodon
            try:
                account_info = await mastodon_service.get_account_info()
                if account_info.get("success"):
                    self.system_status["modules_status"]["mastodon"] = "connected"
                    logger.info("✅ Mastodon API connection verified")
                else:
                    raise Exception(account_info.get("error", "Unknown error"))
            except Exception as e:
                logger.error(f"❌ Mastodon connection failed: {e}")
                self.system_status["modules_status"]["mastodon"] = "error"
                self.system_status["errors"].append(f"Mastodon connection failed: {e}")
                
        except Exception as e:
            logger.error(f"❌ Error testing social connections: {e}")
    
    async def _test_paypal_connection(self):
        """Test PayPal API connection."""
        try:
            # Test by getting access token
            token = await paypal_service._get_access_token()
            if token:
                self.system_status["modules_status"]["paypal"] = "connected"
                logger.info("✅ PayPal API connection verified")
            else:
                raise Exception("Failed to get access token")
                
        except Exception as e:
            logger.error(f"❌ PayPal connection failed: {e}")
            self.system_status["modules_status"]["paypal"] = "error"
            self.system_status["errors"].append(f"PayPal connection failed: {e}")
    
    async def _test_ai_service(self):
        """Test AI service connection."""
        try:
            # Test with a simple request
            response = await ai_service.generate_response(
                "Test connection", 
                max_tokens=10, 
                temperature=0.1
            )
            
            if response.get("content"):
                self.system_status["modules_status"]["ai"] = "connected"
                logger.info("✅ AI service connection verified")
            else:
                raise Exception("No response from AI service")
                
        except Exception as e:
            logger.error(f"❌ AI service connection failed: {e}")
            self.system_status["modules_status"]["ai"] = "error"
            self.system_status["errors"].append(f"AI service connection failed: {e}")
    
    async def start(self):
        """Start the AURELIUS system and begin autonomous operations."""
        try:
            if not await self.initialize():
                logger.error("❌ System initialization failed, cannot start")
                return False
            
            self.running = True
            logger.info("🚀 AURELIUS autonomous system starting...")
            
            # Start scheduled tasks
            await self._start_scheduled_tasks()
            
            # Start main event loop
            await self._run_main_loop()
            
        except KeyboardInterrupt:
            logger.info("🛑 Shutdown signal received")
            await self.shutdown()
        except Exception as e:
            logger.error(f"❌ System error: {e}")
            await self.shutdown()
    
    async def _start_scheduled_tasks(self):
        """Start all scheduled background tasks."""
        try:
            logger.info("⏰ Starting scheduled tasks...")
            
            # Social media posting task
            if self.scheduled_tasks["social_posting"]["enabled"]:
                task = asyncio.create_task(self._social_posting_loop())
                self.tasks.append(task)
                logger.info("📱 Social posting task started")
            
            # Engagement monitoring task
            if self.scheduled_tasks["engagement_monitoring"]["enabled"]:
                task = asyncio.create_task(self._engagement_monitoring_loop())
                self.tasks.append(task)
                logger.info("💬 Engagement monitoring task started")
            
            # Sales processing task
            if self.scheduled_tasks["sales_processing"]["enabled"]:
                task = asyncio.create_task(self._sales_processing_loop())
                self.tasks.append(task)
                logger.info("💰 Sales processing task started")
            
            # Analytics generation task
            if self.scheduled_tasks["analytics_generation"]["enabled"]:
                task = asyncio.create_task(self._analytics_loop())
                self.tasks.append(task)
                logger.info("📊 Analytics task started")
            
            # Learning cycle task
            if self.scheduled_tasks["learning_cycle"]["enabled"]:
                task = asyncio.create_task(self._learning_loop())
                self.tasks.append(task)
                logger.info("🧠 Learning cycle task started")
            
            # Health check task
            if self.scheduled_tasks["health_check"]["enabled"]:
                task = asyncio.create_task(self._health_check_loop())
                self.tasks.append(task)
                logger.info("🏥 Health check task started")
            
            logger.info(f"✅ Started {len(self.tasks)} scheduled tasks")
            
        except Exception as e:
            logger.error(f"❌ Error starting scheduled tasks: {e}")
    
    async def _run_main_loop(self):
        """Main system event loop."""
        try:
            logger.info("🔄 Main event loop started")
            
            while self.running:
                # Update system uptime
                if self.system_status["started_at"]:
                    start_time = datetime.fromisoformat(self.system_status["started_at"])
                    self.system_status["uptime"] = (datetime.now() - start_time).total_seconds()
                
                # Check for completed tasks
                completed_tasks = [task for task in self.tasks if task.done()]
                for task in completed_tasks:
                    try:
                        await task  # This will raise any exceptions
                    except Exception as e:
                        logger.error(f"❌ Task completed with error: {e}")
                        self.system_status["errors"].append(f"Task error: {e}")
                    
                    self.tasks.remove(task)
                
                # Sleep before next iteration
                await asyncio.sleep(10)  # Check every 10 seconds
                
        except Exception as e:
            logger.error(f"❌ Main loop error: {e}")
            self.running = False
    
    async def _social_posting_loop(self):
        """Automated social media posting loop."""
        try:
            while self.running:
                try:
                    logger.info("📱 Running social media posting cycle...")
                    
                    # Generate content for each platform
                    platforms = ["twitter", "mastodon", "discord"]
                    
                    for platform in platforms:
                        try:
                            # Check rate limits
                            if not await rate_limiter.is_action_allowed(platform, "post"):
                                logger.info(f"⏸️  Skipping {platform} - rate limit reached")
                                continue
                            
                            # Generate content using AI
                            topic = await self._get_content_topic()
                            content = await ai_service.generate_social_content(
                                topic=topic,
                                platform=platform,
                                tone="professional",
                                include_hashtags=True
                            )
                            
                            if not content:
                                logger.warning(f"⚠️  No content generated for {platform}")
                                continue
                            
                            # Post to platform
                            result = await self._post_to_platform(platform, content)
                            
                            if result.get("success"):
                                logger.info(f"✅ Posted to {platform}: {content[:50]}...")
                            else:
                                logger.error(f"❌ Failed to post to {platform}: {result.get('error')}")
                            
                            # Wait between posts
                            await asyncio.sleep(30)
                            
                        except Exception as e:
                            logger.error(f"❌ Error posting to {platform}: {e}")
                    
                    # Wait before next cycle (1 hour)
                    await asyncio.sleep(3600)
                    
                except Exception as e:
                    logger.error(f"❌ Social posting cycle error: {e}")
                    await asyncio.sleep(300)  # Wait 5 minutes on error
                    
        except asyncio.CancelledError:
            logger.info("📱 Social posting task cancelled")
        except Exception as e:
            logger.error(f"❌ Social posting loop error: {e}")
    
    async def _engagement_monitoring_loop(self):
        """Monitor and respond to social media engagement."""
        try:
            while self.running:
                try:
                    logger.info("💬 Monitoring social media engagement...")
                    
                    # Check Twitter mentions
                    try:
                        mentions = await twitter_service.get_mentions(max_results=10)
                        for mention in mentions:
                            if not mention.get("processed"):
                                await self._handle_twitter_mention(mention)
                    except Exception as e:
                        logger.error(f"❌ Error checking Twitter mentions: {e}")
                    
                    # Check Mastodon notifications
                    try:
                        notifications = await mastodon_service.get_notifications(limit=10)
                        for notification in notifications:
                            if not notification.get("processed"):
                                await self._handle_mastodon_notification(notification)
                    except Exception as e:
                        logger.error(f"❌ Error checking Mastodon notifications: {e}")
                    
                    # Wait before next check (15 minutes)
                    await asyncio.sleep(900)
                    
                except Exception as e:
                    logger.error(f"❌ Engagement monitoring cycle error: {e}")
                    await asyncio.sleep(300)
                    
        except asyncio.CancelledError:
            logger.info("💬 Engagement monitoring task cancelled")
        except Exception as e:
            logger.error(f"❌ Engagement monitoring loop error: {e}")
    
    async def _sales_processing_loop(self):
        """Process sales and payment-related tasks."""
        try:
            while self.running:
                try:
                    logger.info("💰 Processing sales tasks...")
                    
                    # This would include:
                    # - Processing pending orders
                    # - Sending follow-up messages
                    # - Handling customer inquiries
                    # - Updating sales analytics
                    
                    # For now, just log the cycle
                    logger.info("💰 Sales processing cycle completed")
                    
                    # Wait before next cycle (5 minutes)
                    await asyncio.sleep(300)
                    
                except Exception as e:
                    logger.error(f"❌ Sales processing cycle error: {e}")
                    await asyncio.sleep(60)
                    
        except asyncio.CancelledError:
            logger.info("💰 Sales processing task cancelled")
        except Exception as e:
            logger.error(f"❌ Sales processing loop error: {e}")
    
    async def _analytics_loop(self):
        """Generate analytics reports."""
        try:
            while self.running:
                try:
                    logger.info("📊 Generating analytics reports...")
                    
                    # Generate daily report
                    daily_report = await self.analytics.generate_daily_report()
                    if not daily_report.get("error"):
                        logger.info("✅ Daily analytics report generated")
                    
                    # Check if it's time for weekly report (Mondays)
                    if datetime.now().weekday() == 0:  # Monday
                        weekly_report = await self.analytics.generate_weekly_report()
                        if not weekly_report.get("error"):
                            logger.info("✅ Weekly analytics report generated")
                    
                    # Check if it's time for monthly report (1st of month)
                    if datetime.now().day == 1:
                        monthly_report = await self.analytics.generate_monthly_report()
                        if not monthly_report.get("error"):
                            logger.info("✅ Monthly analytics report generated")
                    
                    # Wait 24 hours before next daily report
                    await asyncio.sleep(86400)
                    
                except Exception as e:
                    logger.error(f"❌ Analytics cycle error: {e}")
                    await asyncio.sleep(3600)  # Wait 1 hour on error
                    
        except asyncio.CancelledError:
            logger.info("📊 Analytics task cancelled")
        except Exception as e:
            logger.error(f"❌ Analytics loop error: {e}")
    
    async def _learning_loop(self):
        """Run auto-learning cycles."""
        try:
            while self.running:
                try:
                    logger.info("🧠 Running auto-learning cycle...")
                    
                    # Run learning cycle
                    learning_results = await self.learner.run_learning_cycle()
                    
                    if not learning_results.get("error"):
                        insights_count = len(learning_results.get("insights_generated", []))
                        recommendations_count = len(learning_results.get("recommendations_updated", []))
                        logger.info(f"✅ Learning cycle completed | Insights: {insights_count} | Recommendations: {recommendations_count}")
                    else:
                        logger.error(f"❌ Learning cycle failed: {learning_results.get('error')}")
                    
                    # Wait 1 week before next learning cycle
                    await asyncio.sleep(604800)
                    
                except Exception as e:
                    logger.error(f"❌ Learning cycle error: {e}")
                    await asyncio.sleep(86400)  # Wait 1 day on error
                    
        except asyncio.CancelledError:
            logger.info("🧠 Learning task cancelled")
        except Exception as e:
            logger.error(f"❌ Learning loop error: {e}")
    
    async def _health_check_loop(self):
        """Perform system health checks."""
        try:
            while self.running:
                try:
                    logger.info("🏥 Performing system health check...")
                    
                    health_status = await self._perform_health_check()
                    self.system_status["last_health_check"] = datetime.now().isoformat()
                    
                    # Log health status
                    if health_status.get("overall_health", 0) > 80:
                        logger.info(f"✅ System health: {health_status['overall_health']:.1f}%")
                    else:
                        logger.warning(f"⚠️  System health: {health_status['overall_health']:.1f}%")
                    
                    # Wait 5 minutes before next check
                    await asyncio.sleep(300)
                    
                except Exception as e:
                    logger.error(f"❌ Health check error: {e}")
                    await asyncio.sleep(60)
                    
        except asyncio.CancelledError:
            logger.info("🏥 Health check task cancelled")
        except Exception as e:
            logger.error(f"❌ Health check loop error: {e}")
    
    async def _perform_health_check(self) -> Dict[str, Any]:
        """Perform comprehensive system health check."""
        try:
            health_metrics = {
                "modules": {},
                "performance": {},
                "errors": len(self.system_status["errors"]),
                "uptime": self.system_status["uptime"],
                "overall_health": 0
            }
            
            # Check module health
            total_modules = len(self.system_status["modules_status"])
            healthy_modules = 0
            
            for module, status in self.system_status["modules_status"].items():
                is_healthy = status in ["connected", "active"]
                health_metrics["modules"][module] = {
                    "status": status,
                    "healthy": is_healthy
                }
                if is_healthy:
                    healthy_modules += 1
            
            # Calculate overall health
            module_health = (healthy_modules / total_modules * 100) if total_modules > 0 else 0
            error_penalty = min(len(self.system_status["errors"]) * 5, 50)  # Max 50% penalty
            
            health_metrics["overall_health"] = max(0, module_health - error_penalty)
            
            return health_metrics
            
        except Exception as e:
            logger.error(f"❌ Error performing health check: {e}")
            return {"error": str(e), "overall_health": 0}
    
    async def _get_content_topic(self) -> str:
        """Get a topic for content generation."""
        # This could be enhanced to use trending topics, user interests, etc.
        topics = [
            "business automation tips",
            "AI and productivity",
            "social media marketing strategies",
            "entrepreneurship insights",
            "digital transformation",
            "customer engagement best practices",
            "sales optimization techniques",
            "technology trends for business"
        ]
        
        import random
        return random.choice(topics)
    
    async def _post_to_platform(self, platform: str, content: str) -> Dict[str, Any]:
        """Post content to specified platform."""
        try:
            if platform == "twitter":
                return await twitter_service.post_tweet(content)
            elif platform == "mastodon":
                return await mastodon_service.post_status(content)
            elif platform == "discord":
                return await discord_service.send_webhook_message(content)
            else:
                return {"success": False, "error": f"Unknown platform: {platform}"}
                
        except Exception as e:
            return {"success": False, "error": str(e)}
    
    async def _handle_twitter_mention(self, mention: Dict[str, Any]):
        """Handle Twitter mention with AI-generated response."""
        try:
            # Generate response using AI
            response = await ai_service.generate_auto_reply(
                original_message=mention["text"],
                context="Twitter mention response",
                platform="twitter",
                reply_type="helpful"
            )
            
            if response:
                # Reply to the mention
                result = await twitter_service.reply_to_tweet(mention["id"], response)
                if result.get("success"):
                    logger.info(f"✅ Replied to Twitter mention from @{mention.get('author', {}).get('username', 'unknown')}")
                else:
                    logger.error(f"❌ Failed to reply to Twitter mention: {result.get('error')}")
            
        except Exception as e:
            logger.error(f"❌ Error handling Twitter mention: {e}")
    
    async def _handle_mastodon_notification(self, notification: Dict[str, Any]):
        """Handle Mastodon notification with appropriate response."""
        try:
            if notification["type"] == "mention":
                # Generate response using AI
                status = notification.get("status", {})
                response = await ai_service.generate_auto_reply(
                    original_message=status.get("content", ""),
                    context="Mastodon mention response",
                    platform="mastodon",
                    reply_type="helpful"
                )
                
                if response:
                    # Reply to the mention
                    result = await mastodon_service.reply_to_status(status.get("id"), response)
                    if result.get("success"):
                        logger.info(f"✅ Replied to Mastodon mention from @{notification['account']['username']}")
                    else:
                        logger.error(f"❌ Failed to reply to Mastodon mention: {result.get('error')}")
            
        except Exception as e:
            logger.error(f"❌ Error handling Mastodon notification: {e}")
    
    async def shutdown(self):
        """Gracefully shutdown the AURELIUS system."""
        try:
            logger.info("🛑 Shutting down AURELIUS system...")
            self.running = False
            
            # Cancel all tasks
            for task in self.tasks:
                task.cancel()
            
            # Wait for tasks to complete
            if self.tasks:
                await asyncio.gather(*self.tasks, return_exceptions=True)
            
            # Close services
            logger.info("🔌 Closing services...")
            
            try:
                await stop_discord_bot()
            except Exception as e:
                logger.error(f"❌ Error stopping Discord bot: {e}")
            
            try:
                await close_ai_service()
            except Exception as e:
                logger.error(f"❌ Error closing AI service: {e}")
            
            try:
                await paypal_service.close()
            except Exception as e:
                logger.error(f"❌ Error closing PayPal service: {e}")
            
            try:
                await discord_service.close()
            except Exception as e:
                logger.error(f"❌ Error closing Discord service: {e}")
            
            try:
                await close_data_client()
            except Exception as e:
                logger.error(f"❌ Error closing data client: {e}")
            
            log_system_shutdown()
            logger.info("✅ AURELIUS system shutdown complete")
            
        except Exception as e:
            logger.error(f"❌ Error during shutdown: {e}")
    
    def get_system_status(self) -> Dict[str, Any]:
        """Get current system status."""
        return self.system_status.copy()

# Global system instance
aurelius_system = AureliusSystem()

async def main():
    """Main entry point for AURELIUS system."""
    try:
        # Set up signal handlers for graceful shutdown
        def signal_handler(signum, frame):
            logger.info(f"🛑 Received signal {signum}")
            asyncio.create_task(aurelius_system.shutdown())
        
        signal.signal(signal.SIGINT, signal_handler)
        signal.signal(signal.SIGTERM, signal_handler)
        
        # Start the system
        await aurelius_system.start()
        
    except KeyboardInterrupt:
        logger.info("🛑 Keyboard interrupt received")
    except Exception as e:
        logger.error(f"❌ Fatal error: {e}")
    finally:
        await aurelius_system.shutdown()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n🛑 AURELIUS system interrupted")
    except Exception as e:
        print(f"❌ Fatal error: {e}")
    finally:
        print("👋 AURELIUS system stopped")
