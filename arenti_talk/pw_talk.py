"""Playwright talkback: login via API → build URL → open camera → talkback with fake mic."""
import asyncio, os, json, sys
sys.path.insert(0, '/srv/nas/arenti_talk')

EMAIL = os.environ.get('ARENTI_USER', '')
PASSWD = os.environ.get('ARENTI_PASS', '')
DEVICE_CODE = 'ppsc8c8779830131445a'

async def main():
    from auth import ArentiSession
    from playwright.async_api import async_playwright

    # Get real auth token
    sess = ArentiSession(EMAIL, PASSWD)
    await sess.login()
    print(f'[0] Logged in: userId={sess.user_id} token={sess.user_token[:20]}...')

    async with async_playwright() as p:
        browser = await p.chromium.launch(
            headless=True,
            args=[
                '--no-sandbox',
                '--use-fake-device-for-media-stream',
                '--use-fake-ui-for-media-stream',
            ]
        )
        ctx = await browser.new_context(permissions=['microphone', 'camera'])
        page = await ctx.new_page()

        page.on('websocket', lambda ws: print(f'[WS] opened: {ws.url[:60]}'))

        # Build the authenticated URL (base64 encode token like the app does)
        import base64
        # The app puts encrypted user data in the URL — use the API to get the real URL
        # Actually just navigate to the base page and inject auth state
        print('[1] Loading page...')
        await page.goto('https://web-eu.arenti.net/', wait_until='domcontentloaded', timeout=15000)
        await page.wait_for_timeout(1000)

        frames = page.frames
        app_frame = next((f for f in frames if 'page.html' in f.url and 'user=' in f.url), None)
        if not app_frame:
            app_frame = next((f for f in frames if 'page.html' in f.url), None)
        if not app_frame:
            app_frame = page.main_frame
        print(f'[1] App frame url: {app_frame.url[:80]}')

        # Inject auth into localStorage then reload
        await app_frame.evaluate(f"""() => {{
            localStorage.setItem('userId', '{sess.user_id}');
            localStorage.setItem('userToken', '{sess.user_token}');
            localStorage.setItem('token', '{sess.user_token}');
        }}""")

        # Reload to apply auth
        await page.reload(wait_until='networkidle', timeout=20000)
        await page.wait_for_timeout(3000)

        frames = page.frames
        app_frame = next((f for f in frames if 'page.html' in f.url and 'user=' in f.url), None)
        if not app_frame:
            app_frame = page.main_frame
        print(f'[2] After reload, frame: {app_frame.url[:80]}')

        await page.screenshot(path='/tmp/pw_01.png')

        # Check if logged in by looking for device list
        html = await app_frame.inner_text('body') if app_frame else ''
        print(f'[2] Body text preview: {html[:200]}')

        await browser.close()
    await sess.close()

asyncio.run(main())
