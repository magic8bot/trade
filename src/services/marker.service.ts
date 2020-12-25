import { Trade } from 'ccxt'
import { StoreOpts, MarkerModel, Marker } from '@magic8bot/db'
import { chunkedMax, chunkedMin } from '@magic8bot/utils'

export class MarkerService {
  public static async getNextBackMarker(storeOpts: StoreOpts) {
    const marker = await MarkerService.findLatestTradeMarker(storeOpts)
    if (!marker || !marker.from) return null

    const nextMarker = await MarkerModel.findInRange(storeOpts, marker.from - 1)
    if (!nextMarker) return marker.from

    MarkerService.saveMarker(nextMarker)
    return MarkerService.getNextBackMarker(storeOpts)
  }

  public static async getNextForwardMarker(storeOpts: StoreOpts, target: number) {
    const marker = await MarkerModel.findInRange(storeOpts, target)
    if (marker) return MarkerService.getNextForwardMarker(storeOpts, marker.to + 1)
    return target
  }

  public static async saveRawMarker(storeOpts: StoreOpts, to: number, from: number, trades: Trade[]) {
    const marker = MarkerService.makeMarker(storeOpts, to, from, trades)
    await MarkerService.saveMarker(marker)

    return marker
  }

  public static saveMarker(marker: Marker) {
    return MarkerModel.saveMarker(marker)
  }

  public static async findLatestTradeMarker(storeOpts: StoreOpts) {
    return MarkerModel.findLatestTradeMarker(storeOpts)
  }

  private static makeMarker({ exchange, symbol }: StoreOpts, to: number, from: number, trades: Trade[]) {
    const newestTime = chunkedMax(trades.map(({ timestamp }) => timestamp))
    const oldestTime = chunkedMin(trades.map(({ timestamp }) => timestamp))

    return { exchange, symbol, to, from, oldestTime, newestTime }
  }
}
