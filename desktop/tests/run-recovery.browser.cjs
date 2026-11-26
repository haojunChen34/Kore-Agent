// Run against an isolated Vite server; RPCs are replaced before mounting the real component.
// VITE_TEST_URL=http://127.0.0.1:15186 PLAYWRIGHT_MODULE=<playwright-core> node tests/run-recovery.browser.cjs
const assert = require('node:assert/strict')
const { chromium } = require(process.env.PLAYWRIGHT_MODULE || 'playwright-core')
const base = process.env.VITE_TEST_URL || 'http://127.0.0.1:15186'
const html = `<!doctype html><div id="root"></div><script type="module">
import RefreshRuntime from '/@react-refresh';
RefreshRuntime.injectIntoGlobalHook(window);window.$RefreshReg$=()=>{};window.$RefreshSig$=()=>type=>type;window.__vite_plugin_react_preamble_installed__=true;
await import('/tests/run-recovery.fixture.jsx');
</script>`

async function main() {
  const browser = await chromium.launch({ headless: true, ...(process.env.EDGE_PATH ? { executablePath: process.env.EDGE_PATH } : {}) })
  const page = await browser.newPage()
  const failures = []
  page.on('pageerror', e => { failures.push(e.message); console.error('BROWSER_ERROR', e.message) })
  await page.route(base + '/recovery-test', route => route.fulfill({ contentType: 'text/html', body: html }))
  const title = () => page.locator('.run-detail-header h2').innerText()
  const select = id => page.locator('.run-list-select').filter({ has: page.locator('strong', { hasText: new RegExp('^' + id + '$') }) }).click()
  const button = name => page.getByRole('button', { name, exact: true })
  const waitTitle = id => page.waitForFunction(value => document.querySelector('.run-detail-header h2')?.textContent === value, id)
  const noError = async () => assert.equal(await page.getByRole('alert').count(), 0)
  async function start(mode, initialRunId) {
    await page.goto(base + '/recovery-test')
    await page.waitForFunction(() => window.ready)
    await page.evaluate(({mode,initialRunId}) => window.start(mode,initialRunId), {mode,initialRunId})
    await waitTitle('parent')
  }
  try {
    await start('lifecycle')
    await button('中断').click(); await button('恢复运行').waitFor()
    await button('恢复运行').click(); await waitTitle('child')
    await select('parent'); await waitTitle('parent')
    await button('查看后续运行').click(); await waitTitle('child')
    await button('恢复运行').click(); await waitTitle('grandchild'); await noError()
    assert.deepEqual(await page.evaluate(() => window.fixture.calls.filter(c => c.method === 'run.recover').map(c => c.params.run_id)), ['parent','child'])
    console.log('PASS interrupt, recover, navigate old record, then recover the interrupted child')

    await start('interrupt-error')
    await button('中断').click(); await page.getByRole('alert').waitFor()
    await select('other'); await waitTitle('other'); await noError()
    await select('parent'); await waitTitle('parent'); await noError()
    console.log('PASS interrupt errors are also scoped to their record')

    await start('error')
    await button('恢复运行').click()
    await page.getByRole('alert').waitFor()
    await select('other'); await waitTitle('other'); await noError()
    await select('parent'); await waitTitle('parent'); await noError()
    console.log('PASS error is scoped and cleared when switching records')

    await start('late-error')
    await button('恢复运行').click()
    await page.waitForFunction(() => !!window.fixture.release)
    await select('other'); await waitTitle('other')
    await page.evaluate(() => window.fixture.release())
    await page.waitForFunction(() => window.fixture.client.isMutating() === 0)
    await noError(); await select('parent'); await waitTitle('parent'); await noError()
    console.log('PASS late failure cannot contaminate another or reopened record')

    await start('normal', 'parent')
    await button('恢复运行').click(); await waitTitle('child')
    await select('parent'); await waitTitle('parent')
    await button('查看后续运行').click(); await waitTitle('child'); await noError()
    assert.equal(await page.evaluate(() => window.fixture.calls.filter(c => c.method === 'run.recover').length), 1)
    console.log('PASS initial focus does not pin parent; recovery creates once, old entry only navigates')

    for (const mode of ['existing','offpage','stale']) {
      await start(mode)
      await button(mode === 'stale' ? '恢复运行' : '查看后续运行').click()
      await waitTitle('child'); await noError()
      assert.equal(await page.evaluate(() => window.fixture.calls.filter(c => c.method === 'run.recover').length), mode === 'stale' ? 1 : 0)
      console.log('PASS existing recovery navigation: ' + mode)
    }

    await start('deleted')
    assert.equal(await button('后续运行已删除').isDisabled(), true)
    assert.equal(await button('恢复运行').count(), 0)
    console.log('PASS deleting successor does not enable a second recovery')

    await start('late-success')
    await button('恢复运行').click(); await page.waitForFunction(() => !!window.fixture.release)
    await select('other'); await waitTitle('other')
    await page.evaluate(() => window.fixture.release())
    await page.waitForFunction(() => window.fixture.client.isMutating() === 0)
    assert.equal(await title(),'other'); await noError()
    console.log('PASS late success does not hijack current selection')
    assert.deepEqual(failures, [])
  } finally { await browser.close() }
}
main().catch(error => { console.error(error); process.exitCode = 1 })
