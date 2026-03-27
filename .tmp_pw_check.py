import asyncio
from playwright.async_api import async_playwright

async def main():
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        page = await browser.new_page()
        page.on('console', lambda msg: print('console', msg.type, msg.text))
        page.on('pageerror', lambda err: print('pageerror', err))
        resp = await page.goto('http://127.0.0.1:8765/', wait_until='domcontentloaded', timeout=30000)
        print('status', resp.status if resp else None)
        print('title', await page.title())
        print('body_len', len(await page.inner_html('body')))
        await page.screenshot(path='H:/source/repos/Resistance/output/playwright-dashboard.png', full_page=True)
        await browser.close()

asyncio.run(main())
