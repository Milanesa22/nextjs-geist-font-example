"""
AURELIUS AI Core Module
Handles OpenAI/OpenRouter API integration for content generation and AI responses.
"""

import asyncio
import json
from typing import Dict, List, Optional, Any, Union
import httpx
from datetime import datetime
import base64

from ..config import config
from ..logging_config import get_logger, log_ai_interaction
from ..utils.security import sanitize_for_social, validate_and_sanitize_input

logger = get_logger("AI")

class AureliusAI:
    """
    AI service for content generation, sales copy, and automated responses.
    Supports OpenAI GPT-4/GPT-4o via OpenRouter or direct OpenAI API.
    """
    
    def __init__(self):
        self.api_key = config.OPENROUTER_API_KEY or config.OPENAI_API_KEY
        self.base_url = config.OPENAI_BASE_URL
        self.model = config.OPENAI_MODEL
        self.default_system_prompt = config.DEFAULT_SYSTEM_PROMPT
        self.sales_prompt_template = config.SALES_PROMPT_TEMPLATE
        
        # HTTP client for async requests
        self.client = None
        self._initialize_client()
    
    def _initialize_client(self):
        """Initialize HTTP client with proper headers."""
        headers = {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json",
        }
        
        # Add OpenRouter specific headers if using OpenRouter
        if "openrouter.ai" in self.base_url:
            headers["HTTP-Referer"] = "https://aurelius-ai.com"
            headers["X-Title"] = "AURELIUS AI Business Manager"
        
        self.client = httpx.AsyncClient(
            headers=headers,
            timeout=httpx.Timeout(60.0),
            limits=httpx.Limits(max_keepalive_connections=5, max_connections=10)
        )
    
    async def close(self):
        """Close HTTP client."""
        if self.client:
            await self.client.aclose()
    
    async def generate_response(
        self,
        prompt: str,
        system_prompt: Optional[str] = None,
        max_tokens: int = 1000,
        temperature: float = 0.7,
        model: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Generate AI response using OpenAI/OpenRouter API.
        Returns dict with response, usage info, and metadata.
        """
        try:
            # Sanitize input
            prompt = validate_and_sanitize_input(prompt, "ai_prompt")
            if system_prompt:
                system_prompt = validate_and_sanitize_input(system_prompt, "system_prompt")
            
            # Use provided model or default
            model_to_use = model or self.model
            system_to_use = system_prompt or self.default_system_prompt
            
            # Prepare messages
            messages = [
                {"role": "system", "content": system_to_use},
                {"role": "user", "content": prompt}
            ]
            
            # API request payload
            payload = {
                "model": model_to_use,
                "messages": messages,
                "max_tokens": max_tokens,
                "temperature": temperature,
                "stream": False
            }
            
            logger.info(f"🤖 Generating AI response | Model: {model_to_use} | Tokens: {max_tokens}")
            
            # Make API request
            response = await self.client.post(
                f"{self.base_url}/chat/completions",
                json=payload
            )
            
            if response.status_code != 200:
                error_msg = f"API request failed with status {response.status_code}: {response.text}"
                log_ai_interaction("generation", model_to_use, success=False, error=error_msg)
                raise Exception(error_msg)
            
            # Parse response
            response_data = response.json()
            
            if "choices" not in response_data or not response_data["choices"]:
                error_msg = "No choices in API response"
                log_ai_interaction("generation", model_to_use, success=False, error=error_msg)
                raise Exception(error_msg)
            
            # Extract response content
            content = response_data["choices"][0]["message"]["content"]
            usage = response_data.get("usage", {})
            
            # Log successful interaction
            tokens_used = usage.get("total_tokens", 0)
            log_ai_interaction("generation", model_to_use, tokens_used, success=True)
            
            return {
                "content": content,
                "model": model_to_use,
                "usage": usage,
                "timestamp": datetime.now().isoformat(),
                "prompt_tokens": usage.get("prompt_tokens", 0),
                "completion_tokens": usage.get("completion_tokens", 0),
                "total_tokens": tokens_used
            }
            
        except Exception as e:
            logger.error(f"❌ AI generation failed: {e}")
            log_ai_interaction("generation", model_to_use, success=False, error=str(e))
            raise
    
    async def generate_social_content(
        self,
        topic: str,
        platform: str,
        tone: str = "professional",
        include_hashtags: bool = True,
        target_audience: Optional[str] = None
    ) -> str:
        """
        Generate social media content for specific platform.
        Returns sanitized content ready for posting.
        """
        try:
            # Platform-specific constraints
            platform_limits = {
                "twitter": 280,
                "mastodon": 500,
                "discord": 2000
            }
            
            char_limit = platform_limits.get(platform.lower(), 500)
            
            # Build prompt
            prompt_parts = [
                f"Create engaging {platform} content about: {topic}",
                f"Tone: {tone}",
                f"Character limit: {char_limit}",
                f"Include hashtags: {'Yes' if include_hashtags else 'No'}"
            ]
            
            if target_audience:
                prompt_parts.append(f"Target audience: {target_audience}")
            
            prompt_parts.extend([
                "Requirements:",
                "- Be engaging and authentic",
                "- Follow platform best practices",
                "- Include a clear call-to-action if appropriate",
                "- Stay within character limits",
                "- Use appropriate formatting for the platform"
            ])
            
            prompt = "\n".join(prompt_parts)
            
            # Generate content
            response = await self.generate_response(
                prompt=prompt,
                max_tokens=300,
                temperature=0.8
            )
            
            # Sanitize for platform
            content = sanitize_for_social(response["content"], platform)
            
            logger.info(f"📱 Generated {platform} content | Length: {len(content)} chars")
            return content
            
        except Exception as e:
            logger.error(f"❌ Social content generation failed for {platform}: {e}")
            return ""
    
    async def generate_sales_copy(
        self,
        product: str,
        audience: str,
        copy_type: str = "email",
        urgency_level: str = "medium"
    ) -> str:
        """
        Generate sales copy for products/services.
        Returns persuasive sales content.
        """
        try:
            # Use configured sales prompt template
            base_prompt = self.sales_prompt_template.format(
                product=product,
                audience=audience
            )
            
            # Add copy type specific instructions
            type_instructions = {
                "email": "Create a compelling email sales copy with subject line and body.",
                "dm": "Create a direct message for social media sales outreach.",
                "ad": "Create advertising copy for social media ads.",
                "landing": "Create landing page sales copy with headlines and benefits."
            }
            
            urgency_instructions = {
                "low": "Use subtle urgency and focus on value.",
                "medium": "Include moderate urgency with time-sensitive offers.",
                "high": "Create strong urgency with limited-time offers and scarcity."
            }
            
            full_prompt = f"""
{base_prompt}

Copy Type: {copy_type}
{type_instructions.get(copy_type, "Create persuasive sales copy.")}

Urgency Level: {urgency_level}
{urgency_instructions.get(urgency_level, "")}

Requirements:
- Focus on benefits, not just features
- Address pain points and objections
- Include social proof if relevant
- Have a clear, compelling call-to-action
- Use persuasive but ethical language
- Match the tone to the target audience
"""
            
            response = await self.generate_response(
                prompt=full_prompt,
                max_tokens=800,
                temperature=0.7
            )
            
            # Sanitize content
            content = validate_and_sanitize_input(response["content"], "sales_copy")
            
            logger.info(f"💰 Generated {copy_type} sales copy | Length: {len(content)} chars")
            return content
            
        except Exception as e:
            logger.error(f"❌ Sales copy generation failed: {e}")
            return ""
    
    async def generate_auto_reply(
        self,
        original_message: str,
        context: str,
        platform: str,
        reply_type: str = "helpful"
    ) -> str:
        """
        Generate automated replies to messages/comments.
        Returns appropriate response based on context.
        """
        try:
            # Sanitize input message
            original_message = validate_and_sanitize_input(original_message, "message")
            context = validate_and_sanitize_input(context, "context")
            
            reply_styles = {
                "helpful": "Be helpful, informative, and supportive.",
                "sales": "Be helpful but guide towards sales opportunities.",
                "customer_service": "Be professional and solution-focused.",
                "engagement": "Be engaging and encourage further conversation."
            }
            
            prompt = f"""
Generate an appropriate reply to this {platform} message:

Original Message: "{original_message}"
Context: {context}
Reply Style: {reply_type}
Style Instructions: {reply_styles.get(reply_type, "Be professional and helpful.")}

Requirements:
- Keep response concise and relevant
- Match the tone of the original message
- Be authentic and human-like
- Include helpful information when appropriate
- Follow {platform} communication best practices
- Avoid being overly promotional unless it's a sales reply type
"""
            
            response = await self.generate_response(
                prompt=prompt,
                max_tokens=200,
                temperature=0.8
            )
            
            # Sanitize for platform
            content = sanitize_for_social(response["content"], platform)
            
            logger.info(f"💬 Generated {reply_type} reply for {platform} | Length: {len(content)} chars")
            return content
            
        except Exception as e:
            logger.error(f"❌ Auto-reply generation failed: {e}")
            return ""
    
    async def analyze_content_performance(
        self,
        content: str,
        engagement_data: Dict[str, Any],
        platform: str
    ) -> Dict[str, Any]:
        """
        Analyze content performance and provide optimization suggestions.
        Returns analysis and recommendations.
        """
        try:
            # Sanitize inputs
            content = validate_and_sanitize_input(content, "content")
            engagement_data = validate_and_sanitize_input(engagement_data, "engagement_data")
            
            prompt = f"""
Analyze this {platform} content performance and provide optimization recommendations:

Content: "{content}"
Engagement Data: {json.dumps(engagement_data, indent=2)}

Please analyze:
1. What worked well in this content
2. What could be improved
3. Specific recommendations for future content
4. Optimal posting times/strategies based on engagement
5. Content format suggestions

Provide actionable insights in JSON format with the following structure:
{{
    "performance_score": 1-10,
    "strengths": ["list of strengths"],
    "weaknesses": ["list of areas for improvement"],
    "recommendations": ["specific actionable recommendations"],
    "content_suggestions": ["ideas for future content"],
    "optimization_tips": ["platform-specific optimization tips"]
}}
"""
            
            response = await self.generate_response(
                prompt=prompt,
                max_tokens=600,
                temperature=0.5
            )
            
            # Try to parse as JSON
            try:
                analysis = json.loads(response["content"])
            except json.JSONDecodeError:
                # Fallback to text analysis
                analysis = {
                    "performance_score": 5,
                    "analysis_text": response["content"],
                    "error": "Could not parse structured analysis"
                }
            
            logger.info(f"📊 Analyzed content performance for {platform}")
            return analysis
            
        except Exception as e:
            logger.error(f"❌ Content analysis failed: {e}")
            return {"error": str(e)}
    
    async def generate_content_strategy(
        self,
        business_info: Dict[str, Any],
        target_audience: str,
        goals: List[str],
        platforms: List[str]
    ) -> Dict[str, Any]:
        """
        Generate comprehensive content strategy.
        Returns strategic recommendations and content calendar ideas.
        """
        try:
            # Sanitize inputs
            business_info = validate_and_sanitize_input(business_info, "business_info")
            target_audience = validate_and_sanitize_input(target_audience, "audience")
            
            prompt = f"""
Create a comprehensive content strategy for this business:

Business Information: {json.dumps(business_info, indent=2)}
Target Audience: {target_audience}
Goals: {', '.join(goals)}
Platforms: {', '.join(platforms)}

Please provide a strategic plan including:
1. Content pillars and themes
2. Posting frequency recommendations per platform
3. Content mix (educational, promotional, engaging, etc.)
4. Optimal posting times
5. Engagement strategies
6. Content calendar template
7. KPIs to track
8. Growth strategies

Format as JSON with clear sections and actionable recommendations.
"""
            
            response = await self.generate_response(
                prompt=prompt,
                max_tokens=1200,
                temperature=0.6
            )
            
            # Try to parse as JSON
            try:
                strategy = json.loads(response["content"])
            except json.JSONDecodeError:
                # Fallback to text strategy
                strategy = {
                    "strategy_text": response["content"],
                    "error": "Could not parse structured strategy"
                }
            
            logger.info(f"📋 Generated content strategy for {len(platforms)} platforms")
            return strategy
            
        except Exception as e:
            logger.error(f"❌ Content strategy generation failed: {e}")
            return {"error": str(e)}
    
    async def moderate_content(self, content: str) -> Dict[str, Any]:
        """
        Check content for appropriateness and policy compliance.
        Returns moderation results and recommendations.
        """
        try:
            content = validate_and_sanitize_input(content, "content")
            
            prompt = f"""
Review this content for appropriateness and policy compliance:

Content: "{content}"

Check for:
1. Inappropriate language or content
2. Potential policy violations
3. Spam or promotional issues
4. Misleading information
5. Offensive or harmful content

Provide results in JSON format:
{{
    "approved": true/false,
    "confidence": 0.0-1.0,
    "issues": ["list of any issues found"],
    "recommendations": ["suggestions for improvement"],
    "risk_level": "low/medium/high"
}}
"""
            
            response = await self.generate_response(
                prompt=prompt,
                max_tokens=300,
                temperature=0.3
            )
            
            # Try to parse as JSON
            try:
                moderation = json.loads(response["content"])
            except json.JSONDecodeError:
                # Conservative fallback
                moderation = {
                    "approved": False,
                    "confidence": 0.5,
                    "issues": ["Could not parse moderation results"],
                    "risk_level": "medium"
                }
            
            logger.info(f"🛡️  Content moderation completed | Approved: {moderation.get('approved', False)}")
            return moderation
            
        except Exception as e:
            logger.error(f"❌ Content moderation failed: {e}")
            return {
                "approved": False,
                "confidence": 0.0,
                "issues": [f"Moderation error: {str(e)}"],
                "risk_level": "high"
            }

# Global AI instance
ai_service = AureliusAI()

async def generate_platform_content(topic: str, platform: str, **kwargs) -> str:
    """Quick function to generate content for a platform."""
    return await ai_service.generate_social_content(topic, platform, **kwargs)

async def generate_sales_message(product: str, audience: str, **kwargs) -> str:
    """Quick function to generate sales copy."""
    return await ai_service.generate_sales_copy(product, audience, **kwargs)

async def generate_reply(message: str, context: str, platform: str, **kwargs) -> str:
    """Quick function to generate automated replies."""
    return await ai_service.generate_auto_reply(message, context, platform, **kwargs)

async def close_ai_service():
    """Close AI service HTTP client."""
    await ai_service.close()
