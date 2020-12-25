import { Event, smq } from '@magic8bot/smq'
import { TradeModel, Trade, StoreOpts } from '@magic8bot/db'

const MAX_TRADES_LOAD = 5000

export class TradeService {
  public static async loadTrades({ exchange, symbol }: StoreOpts, isBackfill = false, timestamp = 0) {
    const trades = await TradeModel.findTrades(exchange, symbol, timestamp)

    if (!trades.length) return

    if (isBackfill) {
      smq.publish(Event.XCH_TRADE_PREROLL, `${exchange}.${symbol}`, trades)
      if (trades.length !== MAX_TRADES_LOAD) return

      return this.loadTrades({ exchange, symbol }, isBackfill, trades[trades.length - 1].timestamp)
    }

    trades.forEach((trade) => smq.publish(Event.XCH_TRADE, `${exchange}.${symbol}`, trade))
  }

  public static insertTrades(storeOpts: StoreOpts, newTrades: Trade[]) {
    return TradeModel.insertTrades(storeOpts, newTrades)
  }
}
