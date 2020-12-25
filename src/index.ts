import { ExchangeProvider } from '@magic8bot/adapters'
import { dbDriver, ExchangeModel } from '@magic8bot/db'
import { Channel, smq } from '@magic8bot/smq'
import { sleep } from '@magic8bot/utils'

import { TradeEngine } from './engine'

interface Message {
  exchange: string
  symbol: string
  action: 'start' | 'stop'
}

const exchangeProvider = new ExchangeProvider()

export class Service {
  static syncers: Map<string, TradeEngine> = new Map()

  static async run() {
    await dbDriver.connect('mongo')

    Service.tick()
  }

  private static async tick() {
    await sleep(5000)

    const resp = await smq.receiveMessage<Message>(Channel.SyncTrades)
    if (!resp.message) return Service.tick()

    const shouldDelete = await Service.processMessage(resp.message)
    if (shouldDelete) smq.deleteMessage(Channel.SyncTrades, resp.id)
  }

  private static async processMessage(message: Message) {
    const { exchange, symbol, action } = message

    if (action === 'start') return Service.start(exchange, symbol)
    else if (action === 'stop') return Service.stop(exchange, symbol)

    return true
  }

  private static async start(exchange: string, symbol: string) {
    const idStr = Service.makeIdStr(exchange, symbol)
    const exchangeConfig = await ExchangeModel.load(exchange)

    if (Service.syncers.has(idStr)) return true

    exchangeProvider.addExchange(exchangeConfig)
    Service.syncers.set(idStr, new TradeEngine(exchangeProvider, exchange, exchangeConfig.tradePollInterval))
    Service.syncers.get(idStr).start(symbol, 1)

    return true
  }

  private static stop(exchange: string, symbol: string) {
    const idStr = Service.makeIdStr(exchange, symbol)
    if (!Service.syncers.has(idStr)) return false

    Service.syncers.get(idStr).stop()
    Service.syncers.delete(idStr)

    return true
  }

  private static makeIdStr(exchange: string, symbol: string) {
    return `${exchange}.${symbol}`
  }
}
