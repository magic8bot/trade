import { ExchangeProvider } from '@magic8bot/adapters'
import { sleep, asyncNextTick, chunkedMax, chunkedMin } from '@magic8bot/utils'

import { MarkerService, TradeService } from '../services'

enum SYNC_STATE {
  STOPPED,
  SYNCING,
  READY,
}

export class TradeEngine {
  private readonly scanType: 'back' | 'forward'
  private state: SYNC_STATE = SYNC_STATE.STOPPED

  constructor(private readonly exchangeProvider: ExchangeProvider, private readonly exchange: string, private readonly tradePollInterval: number) {
    this.scanType = this.exchangeProvider.getScan(this.exchange)
  }

  public isReady() {
    return this.state === SYNC_STATE.READY
  }

  public isRunning() {
    return this.state !== SYNC_STATE.STOPPED
  }

  public getState() {
    return this.state
  }

  public async start(symbol: string, days: number) {
    if (this.state !== SYNC_STATE.STOPPED) return

    this.setState(SYNC_STATE.SYNCING)

    await this.scan(symbol, days)
    if (this.state === SYNC_STATE.STOPPED) return

    this.setState(SYNC_STATE.READY)
    await this.tick(symbol)
  }

  public stop() {
    if (this.state === SYNC_STATE.STOPPED) return

    this.setState(SYNC_STATE.STOPPED)
  }

  private setState(state: SYNC_STATE) {
    this.state = state
  }

  private async scan(symbol: string, days: number) {
    const storeOpts = { exchange: this.exchange, symbol }

    const now = this.getNow()
    const target = now - 86400000 * days

    await (this.scanType === 'back' ? this.scanBack(symbol, target) : this.scanForward(symbol, target))
    if (this.state === SYNC_STATE.STOPPED) return

    return TradeService.loadTrades(storeOpts, true)
  }

  private async tick(symbol: string) {
    if (this.state === SYNC_STATE.STOPPED) return

    const storeOpts = { exchange: this.exchange, symbol }
    const target = (await MarkerService.findLatestTradeMarker(storeOpts)).newestTime

    await (this.scanType === 'back' ? this.tickBack(symbol, target) : this.scanForward(symbol, target, true))
    await sleep(this.tradePollInterval)

    await TradeService.loadTrades(storeOpts)
    await this.recursiveTick(symbol)
  }

  private async recursiveTick(symbol: string) {
    await this.tick(symbol)
  }

  private getNow() {
    return new Date().getTime()
  }

  private async scanBack(symbol: string, end: number) {
    if (this.state === SYNC_STATE.STOPPED) return

    const storeOpts = { exchange: this.exchange, symbol }
    // The next "to" is the previous "from"
    const to = await MarkerService.getNextBackMarker(storeOpts)

    const trades = await this.exchangeProvider.getTrades(this.exchange, symbol, to)

    await TradeService.insertTrades(storeOpts, trades)

    const from = chunkedMin(trades.map((trade) => this.exchangeProvider.getTradeCursor(this.exchange, trade)))
    const { oldestTime } = await MarkerService.saveRawMarker(storeOpts, to, from, trades)

    if (oldestTime > end) {
      await this.scanBack(symbol, end)
    }
  }

  private async scanForward(symbol: string, start: number, isTick = false) {
    if (this.state === SYNC_STATE.STOPPED) return
    const now = this.getNow()

    const storeOpts = { exchange: this.exchange, symbol }
    const from = await MarkerService.getNextForwardMarker(storeOpts, start)

    const trades = await this.exchangeProvider.getTrades(this.exchange, symbol, from)

    if (!trades.length) return

    await TradeService.insertTrades(storeOpts, trades)

    const to = chunkedMax(trades.map((trade) => this.exchangeProvider.getTradeCursor(this.exchange, trade)))
    const { newestTime } = await MarkerService.saveRawMarker(storeOpts, to, from, trades)

    // Always get current time so backfill can catch up to "now"
    if (newestTime < now) await asyncNextTick(this.scanForward(symbol, to, isTick))
  }

  private async tickBack(symbol: string, target: number, lastFrom: number = null) {
    if (this.state === SYNC_STATE.STOPPED) return

    const storeOpts = { exchange: this.exchange, symbol }
    const trades = await this.exchangeProvider.getTrades(this.exchange, symbol, lastFrom)

    const filteredTrades = trades.filter(({ timestamp }) => timestamp > target)

    if (!filteredTrades.length) return

    await TradeService.insertTrades(storeOpts, filteredTrades)

    const from = chunkedMin(filteredTrades.map((trade) => this.exchangeProvider.getTradeCursor(this.exchange, trade)))
    const to = chunkedMax(filteredTrades.map((trade) => this.exchangeProvider.getTradeCursor(this.exchange, trade)))
    await MarkerService.saveRawMarker(storeOpts, to, from, filteredTrades)

    if (filteredTrades.length === trades.length) await this.tickBack(symbol, target, from)
  }
}
