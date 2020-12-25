import { ExchangeProvider } from '@magic8bot/adapters'
import { dbDriver, ExchangeModel } from '@magic8bot/db'
import { Channel, smq } from '@magic8bot/smq'

import { TradeEngine } from './engine'
import { sleep } from './utils'

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
    const message = await smq.receiveMessage<Message>(Channel.SyncTrades)
    if (message) Service.processMessage(message)

    await sleep(5000)

    Service.tick()
  }

  private static processMessage(message: Message) {
    const { exchange, symbol, action } = message

    if (action === 'start') Service.start(exchange, symbol)
    else if (action === 'stop') Service.stop(exchange, symbol)
  }

  private static async start(exchange: string, symbol: string) {
    const idStr = Service.makeIdStr(exchange, symbol)
    const exchangeConfig = await ExchangeModel.load(exchange)

    if (Service.syncers.has(idStr)) return

    exchangeProvider.addExchange(exchangeConfig)
    Service.syncers.set(idStr, new TradeEngine(exchangeProvider, exchange, exchangeConfig.tradePollInterval))
    Service.syncers.get(idStr).start(symbol, 1)
  }

  private static stop(exchange: string, symbol: string) {
    const idStr = Service.makeIdStr(exchange, symbol)
    if (!Service.syncers.has(idStr)) return

    Service.syncers.get(idStr).stop()
    Service.syncers.delete(idStr)
  }

  private static makeIdStr(exchange: string, symbol: string) {
    return `${exchange}.${symbol}`
  }
}
