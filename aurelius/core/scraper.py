"""
AURELIUS Web Scraper Module
Handles web scraping for market research and competitor analysis.
"""

import asyncio
import aiohttp
from typing import Dict, List, Optional, Any, Union
from urllib.parse import urljoin, urlparse
from bs4 import BeautifulSoup
import re
from datetime import datetime
import json

from ..config import config
from ..logging_config import get_logger, log_data_operation
from ..utils.security import validate_and_sanitize_input, SecurityValidator

logger = get_logger("SCRAPER")

class AureliusScraper:
    """
    Async web scraper for market research and competitor analysis.
    Includes rate limiting and error handling.
    """
    
    def __init__(self):
        self.session = None
        self.user_agent = "AURELIUS-Bot/1.0 (Business Research)"
        self.timeout = aiohttp.ClientTimeout(total=30)
        self.max_retries = 3
        self.delay_between_requests = 1.0  # seconds
        
        # Common selectors for different content types
        self.content_selectors = {
            'title': ['title', 'h1', '.title', '#title'],
            'description': ['meta[name="description"]', '.description', '.summary'],
            'content': ['article', '.content', '.post-content', 'main', '.entry-content'],
            'price': ['.price', '.cost', '[data-price]', '.amount'],
            'social_links': ['a[href*="twitter.com"]', 'a[href*="facebook.com"]', 'a[href*="instagram.com"]']
        }
    
    async def __aenter__(self):
        """Async context manager entry."""
        await self.start_session()
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit."""
        await self.close_session()
    
    async def start_session(self):
        """Initialize aiohttp session."""
        if not self.session:
            headers = {
                'User-Agent': self.user_agent,
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
                'Accept-Language': 'en-US,en;q=0.5',
                'Accept-Encoding': 'gzip, deflate',
                'Connection': 'keep-alive',
                'Upgrade-Insecure-Requests': '1',
            }
            
            self.session = aiohttp.ClientSession(
                headers=headers,
                timeout=self.timeout,
                connector=aiohttp.TCPConnector(limit=10, limit_per_host=5)
            )
            
            logger.info("🕷️  Scraper session initialized")
    
    async def close_session(self):
        """Close aiohttp session."""
        if self.session:
            await self.session.close()
            self.session = None
            logger.info("🕷️  Scraper session closed")
    
    async def fetch_page(self, url: str, retries: int = 0) -> Optional[str]:
        """
        Fetch a single page with error handling and retries.
        Returns HTML content or None if failed.
        """
        try:
            # Validate URL
            if not SecurityValidator.validate_url(url):
                logger.error(f"❌ Invalid URL: {url}")
                return None
            
            if not self.session:
                await self.start_session()
            
            logger.info(f"🌐 Fetching: {url}")
            
            async with self.session.get(url) as response:
                if response.status == 200:
                    content = await response.text()
                    logger.info(f"✅ Successfully fetched {url} | Size: {len(content)} chars")
                    log_data_operation("FETCH", "webpage", success=True)
                    return content
                elif response.status == 429:  # Rate limited
                    if retries < self.max_retries:
                        wait_time = (2 ** retries) * self.delay_between_requests
                        logger.warning(f"⚠️  Rate limited, waiting {wait_time}s before retry")
                        await asyncio.sleep(wait_time)
                        return await self.fetch_page(url, retries + 1)
                    else:
                        logger.error(f"❌ Rate limit exceeded for {url}")
                        return None
                else:
                    logger.error(f"❌ HTTP {response.status} for {url}")
                    log_data_operation("FETCH", "webpage", success=False, error=f"HTTP {response.status}")
                    return None
                    
        except asyncio.TimeoutError:
            logger.error(f"❌ Timeout fetching {url}")
            if retries < self.max_retries:
                return await self.fetch_page(url, retries + 1)
            return None
        except Exception as e:
            logger.error(f"❌ Error fetching {url}: {e}")
            log_data_operation("FETCH", "webpage", success=False, error=str(e))
            if retries < self.max_retries:
                await asyncio.sleep(self.delay_between_requests)
                return await self.fetch_page(url, retries + 1)
            return None
    
    def parse_content(self, html: str, url: str) -> Dict[str, Any]:
        """
        Parse HTML content and extract structured data.
        Returns dictionary with extracted information.
        """
        try:
            soup = BeautifulSoup(html, 'html.parser')
            
            # Remove script and style elements
            for script in soup(["script", "style"]):
                script.decompose()
            
            extracted_data = {
                'url': url,
                'timestamp': datetime.now().isoformat(),
                'title': '',
                'description': '',
                'content': '',
                'meta_data': {},
                'links': [],
                'images': [],
                'social_links': [],
                'contact_info': {},
                'prices': [],
                'headings': []
            }
            
            # Extract title
            title_elem = soup.find('title')
            if title_elem:
                extracted_data['title'] = validate_and_sanitize_input(title_elem.get_text().strip())
            
            # Extract meta description
            meta_desc = soup.find('meta', attrs={'name': 'description'})
            if meta_desc:
                extracted_data['description'] = validate_and_sanitize_input(meta_desc.get('content', ''))
            
            # Extract main content
            content_text = ""
            for selector in self.content_selectors['content']:
                content_elem = soup.select_one(selector)
                if content_elem:
                    content_text = content_elem.get_text(separator=' ', strip=True)
                    break
            
            if content_text:
                extracted_data['content'] = validate_and_sanitize_input(content_text[:5000])  # Limit content length
            
            # Extract headings
            for i in range(1, 7):  # h1 to h6
                headings = soup.find_all(f'h{i}')
                for heading in headings:
                    heading_text = validate_and_sanitize_input(heading.get_text().strip())
                    if heading_text:
                        extracted_data['headings'].append({
                            'level': i,
                            'text': heading_text
                        })
            
            # Extract links
            links = soup.find_all('a', href=True)
            for link in links[:50]:  # Limit number of links
                href = link.get('href')
                if href:
                    full_url = urljoin(url, href)
                    if SecurityValidator.validate_url(full_url):
                        extracted_data['links'].append({
                            'url': full_url,
                            'text': validate_and_sanitize_input(link.get_text().strip())
                        })
            
            # Extract images
            images = soup.find_all('img', src=True)
            for img in images[:20]:  # Limit number of images
                src = img.get('src')
                if src:
                    full_url = urljoin(url, src)
                    extracted_data['images'].append({
                        'url': full_url,
                        'alt': validate_and_sanitize_input(img.get('alt', ''))
                    })
            
            # Extract social media links
            for selector in self.content_selectors['social_links']:
                social_links = soup.select(selector)
                for link in social_links:
                    href = link.get('href')
                    if href and SecurityValidator.validate_url(href):
                        extracted_data['social_links'].append(href)
            
            # Extract potential prices
            price_patterns = [
                r'\$[\d,]+\.?\d*',
                r'€[\d,]+\.?\d*',
                r'£[\d,]+\.?\d*',
                r'[\d,]+\.?\d*\s*USD',
                r'[\d,]+\.?\d*\s*EUR'
            ]
            
            page_text = soup.get_text()
            for pattern in price_patterns:
                prices = re.findall(pattern, page_text)
                extracted_data['prices'].extend(prices[:10])  # Limit prices
            
            # Extract contact information
            email_pattern = r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b'
            phone_pattern = r'(\+\d{1,3}[-.\s]?)?\(?\d{3}\)?[-.\s]?\d{3}[-.\s]?\d{4}'
            
            emails = re.findall(email_pattern, page_text)
            phones = re.findall(phone_pattern, page_text)
            
            if emails:
                extracted_data['contact_info']['emails'] = list(set(emails[:5]))
            if phones:
                extracted_data['contact_info']['phones'] = list(set([phone[0] + phone[1] if isinstance(phone, tuple) else phone for phone in phones[:5]]))
            
            # Extract meta data
            meta_tags = soup.find_all('meta')
            for meta in meta_tags:
                name = meta.get('name') or meta.get('property')
                content = meta.get('content')
                if name and content:
                    extracted_data['meta_data'][name] = validate_and_sanitize_input(content)
            
            logger.info(f"📊 Parsed content from {url} | Title: {extracted_data['title'][:50]}...")
            return extracted_data
            
        except Exception as e:
            logger.error(f"❌ Error parsing content from {url}: {e}")
            return {
                'url': url,
                'timestamp': datetime.now().isoformat(),
                'error': str(e)
            }
    
    async def scrape_url(self, url: str) -> Dict[str, Any]:
        """
        Scrape a single URL and return structured data.
        Main public method for scraping.
        """
        try:
            # Add delay to be respectful
            await asyncio.sleep(self.delay_between_requests)
            
            # Fetch page content
            html_content = await self.fetch_page(url)
            
            if not html_content:
                return {
                    'url': url,
                    'timestamp': datetime.now().isoformat(),
                    'error': 'Failed to fetch page content'
                }
            
            # Parse content
            parsed_data = self.parse_content(html_content, url)
            
            return parsed_data
            
        except Exception as e:
            logger.error(f"❌ Error scraping {url}: {e}")
            return {
                'url': url,
                'timestamp': datetime.now().isoformat(),
                'error': str(e)
            }
    
    async def scrape_multiple_urls(self, urls: List[str], max_concurrent: int = 5) -> List[Dict[str, Any]]:
        """
        Scrape multiple URLs concurrently with rate limiting.
        Returns list of scraped data.
        """
        if not urls:
            return []
        
        logger.info(f"🕷️  Starting to scrape {len(urls)} URLs with max {max_concurrent} concurrent requests")
        
        # Create semaphore to limit concurrent requests
        semaphore = asyncio.Semaphore(max_concurrent)
        
        async def scrape_with_semaphore(url):
            async with semaphore:
                return await self.scrape_url(url)
        
        # Execute scraping tasks
        tasks = [scrape_with_semaphore(url) for url in urls]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        # Process results and handle exceptions
        processed_results = []
        for i, result in enumerate(results):
            if isinstance(result, Exception):
                logger.error(f"❌ Exception scraping {urls[i]}: {result}")
                processed_results.append({
                    'url': urls[i],
                    'timestamp': datetime.now().isoformat(),
                    'error': str(result)
                })
            else:
                processed_results.append(result)
        
        successful_scrapes = len([r for r in processed_results if 'error' not in r])
        logger.info(f"✅ Completed scraping | Success: {successful_scrapes}/{len(urls)}")
        
        return processed_results
    
    async def analyze_competitor(self, competitor_url: str) -> Dict[str, Any]:
        """
        Analyze a competitor website for business intelligence.
        Returns comprehensive competitor analysis.
        """
        try:
            logger.info(f"🔍 Analyzing competitor: {competitor_url}")
            
            # Scrape main page
            main_data = await self.scrape_url(competitor_url)
            
            if 'error' in main_data:
                return main_data
            
            # Analyze the scraped data
            analysis = {
                'competitor_url': competitor_url,
                'analysis_timestamp': datetime.now().isoformat(),
                'basic_info': {
                    'title': main_data.get('title', ''),
                    'description': main_data.get('description', ''),
                    'social_presence': len(main_data.get('social_links', [])) > 0
                },
                'content_analysis': {
                    'content_length': len(main_data.get('content', '')),
                    'heading_count': len(main_data.get('headings', [])),
                    'image_count': len(main_data.get('images', [])),
                    'link_count': len(main_data.get('links', []))
                },
                'pricing_info': {
                    'prices_found': main_data.get('prices', []),
                    'has_pricing': len(main_data.get('prices', [])) > 0
                },
                'contact_info': main_data.get('contact_info', {}),
                'social_links': main_data.get('social_links', []),
                'meta_analysis': {
                    'seo_optimized': bool(main_data.get('description')),
                    'social_meta': any('og:' in key for key in main_data.get('meta_data', {})),
                    'twitter_meta': any('twitter:' in key for key in main_data.get('meta_data', {}))
                },
                'recommendations': []
            }
            
            # Generate recommendations based on analysis
            if not analysis['basic_info']['social_presence']:
                analysis['recommendations'].append("Competitor has limited social media presence - opportunity for better social engagement")
            
            if not analysis['pricing_info']['has_pricing']:
                analysis['recommendations'].append("Competitor doesn't display pricing prominently - consider transparent pricing strategy")
            
            if not analysis['meta_analysis']['seo_optimized']:
                analysis['recommendations'].append("Competitor has poor SEO optimization - opportunity for better search visibility")
            
            logger.info(f"✅ Competitor analysis completed for {competitor_url}")
            return analysis
            
        except Exception as e:
            logger.error(f"❌ Competitor analysis failed for {competitor_url}: {e}")
            return {
                'competitor_url': competitor_url,
                'analysis_timestamp': datetime.now().isoformat(),
                'error': str(e)
            }
    
    async def monitor_keywords(self, keywords: List[str], search_urls: List[str]) -> Dict[str, Any]:
        """
        Monitor keywords across specified URLs for market research.
        Returns keyword analysis and trends.
        """
        try:
            logger.info(f"🔍 Monitoring {len(keywords)} keywords across {len(search_urls)} URLs")
            
            # Scrape all URLs
            scraped_data = await self.scrape_multiple_urls(search_urls)
            
            keyword_analysis = {
                'keywords': keywords,
                'monitored_urls': search_urls,
                'analysis_timestamp': datetime.now().isoformat(),
                'keyword_occurrences': {},
                'trending_topics': [],
                'recommendations': []
            }
            
            # Analyze keyword occurrences
            for keyword in keywords:
                keyword_lower = keyword.lower()
                occurrences = []
                
                for data in scraped_data:
                    if 'error' in data:
                        continue
                    
                    content = (data.get('content', '') + ' ' + data.get('title', '')).lower()
                    count = content.count(keyword_lower)
                    
                    if count > 0:
                        occurrences.append({
                            'url': data['url'],
                            'count': count,
                            'context': self._extract_keyword_context(content, keyword_lower)
                        })
                
                keyword_analysis['keyword_occurrences'][keyword] = {
                    'total_occurrences': sum(occ['count'] for occ in occurrences),
                    'urls_found': len(occurrences),
                    'details': occurrences
                }
            
            # Generate recommendations
            popular_keywords = sorted(
                keyword_analysis['keyword_occurrences'].items(),
                key=lambda x: x[1]['total_occurrences'],
                reverse=True
            )
            
            if popular_keywords:
                top_keyword = popular_keywords[0][0]
                keyword_analysis['recommendations'].append(f"'{top_keyword}' is trending - consider creating content around this topic")
            
            logger.info(f"✅ Keyword monitoring completed")
            return keyword_analysis
            
        except Exception as e:
            logger.error(f"❌ Keyword monitoring failed: {e}")
            return {
                'keywords': keywords,
                'monitored_urls': search_urls,
                'analysis_timestamp': datetime.now().isoformat(),
                'error': str(e)
            }
    
    def _extract_keyword_context(self, content: str, keyword: str, context_length: int = 100) -> List[str]:
        """Extract context around keyword occurrences."""
        contexts = []
        start = 0
        
        while True:
            index = content.find(keyword, start)
            if index == -1:
                break
            
            context_start = max(0, index - context_length // 2)
            context_end = min(len(content), index + len(keyword) + context_length // 2)
            context = content[context_start:context_end].strip()
            
            if context and context not in contexts:
                contexts.append(context)
            
            start = index + 1
            
            if len(contexts) >= 3:  # Limit contexts per keyword
                break
        
        return contexts

# Global scraper instance
scraper_service = AureliusScraper()

async def scrape_competitor_data(competitor_url: str) -> Dict[str, Any]:
    """Quick function to analyze a competitor."""
    async with scraper_service:
        return await scraper_service.analyze_competitor(competitor_url)

async def monitor_market_keywords(keywords: List[str], urls: List[str]) -> Dict[str, Any]:
    """Quick function to monitor keywords."""
    async with scraper_service:
        return await scraper_service.monitor_keywords(keywords, urls)

async def scrape_single_page(url: str) -> Dict[str, Any]:
    """Quick function to scrape a single page."""
    async with scraper_service:
        return await scraper_service.scrape_url(url)
