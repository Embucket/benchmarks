#!/usr/bin/env python3
"""
Script to convert HTML visualizations to PNG images for embedding in README.
Uses Playwright to take screenshots of the HTML files.
"""

import asyncio
import sys
from pathlib import Path
from playwright.async_api import async_playwright


async def capture_screenshot(html_path: str, output_path: str, width: int = 1400, height: int = 2000):
    """Capture a screenshot of an HTML file."""
    html_file = Path(html_path)
    if not html_file.exists():
        print(f"Error: HTML file not found: {html_path}")
        return False
    
    output_file = Path(output_path)
    output_file.parent.mkdir(parents=True, exist_ok=True)
    
    print(f"Capturing screenshot: {html_path} -> {output_path}")
    
    async with async_playwright() as p:
        browser = await p.chromium.launch()
        page = await browser.new_page(viewport={'width': width, 'height': height})
        
        # Load the HTML file
        await page.goto(f'file://{html_file.absolute()}')
        
        # Wait for Mermaid diagram to render
        await page.wait_for_timeout(3000)  # Wait 3 seconds for rendering
        
        # Take screenshot
        await page.screenshot(path=str(output_file), full_page=True)
        
        await browser.close()
    
    print(f"✓ Screenshot saved: {output_path}")
    return True


async def main():
    """Generate screenshots for both visualizations."""
    script_dir = Path(__file__).parent
    visualizations_dir = script_dir.parent.parent / 'visualizations'
    
    # Define input HTML files and output PNG files
    screenshots = [
        {
            'html': script_dir / 'lineage_first_run.html',
            'png': visualizations_dir / 'dbt_snowplow_web_first_run.png',
            'name': 'First Run'
        },
        {
            'html': script_dir / 'lineage_incremental_run.html',
            'png': visualizations_dir / 'dbt_snowplow_web_incremental_run.png',
            'name': 'Incremental Run'
        }
    ]
    
    print("Generating screenshots for dbt-snowplow-web visualizations...")
    print("")
    
    success_count = 0
    for item in screenshots:
        if await capture_screenshot(str(item['html']), str(item['png'])):
            success_count += 1
        print("")
    
    print(f"✓ Generated {success_count}/{len(screenshots)} screenshots")
    
    if success_count == len(screenshots):
        print("\nScreenshots are ready! The README will now display embedded images.")
        return 0
    else:
        print("\nWarning: Some screenshots failed to generate.")
        return 1


if __name__ == '__main__':
    sys.exit(asyncio.run(main()))

