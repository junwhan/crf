package org.hyflow.benchmarks.tpcc

import scala.collection.mutable
import java.util.Date

import scala.concurrent.stm._
import org.hyflow.Hyflow

object TpccOps {
	val rand = new scala.util.Random
	val C = mutable.Map[Int, Int]()
	def NURand(A: Int, x: Int, y: Int): Int = {
		val tmp0 = rand.nextInt(A + 1) | (x + rand.nextInt(y - x + 1))
		((tmp0 + C.getOrElseUpdate(A, rand.nextInt(A))) % (y - x + 1)) + x
	}

	def newOrder(): Double = {
		class LineOrderData {
			var ol_i_id: Int = _
			var ol_supply_w_id: Int = _
			var ol_quantity: Int = _
			var orderLine: TpccOrderLine = _
		}

		// Data Generation
		val w_id = 1
		val d_id = rand.nextInt(10) + 1
		//val c_id = NURand(1023, 1, 3000)
		val c_id = //NURand(103, 1, TpccInit.NUM_CUSTOMERS_PER_D)
			1 + rand.nextInt(TpccInit.NUM_CUSTOMERS_PER_D)

		val ol_cnt = 5 + rand.nextInt(11)
		val rbk = (rand.nextInt(100) == 0)

		val lines = for (i <- 1 to ol_cnt) yield {
			val ol = new LineOrderData
			ol.ol_i_id = if (false /* rbk*/) 0 //else NURand(8191, 1, 100000)
							//else NURand(81, 1, TpccInit.NUM_ITEMS)
							else 1+rand.nextInt(TpccInit.NUM_ITEMS) 
			ol.ol_supply_w_id = 1 // TODO: remote warehouse
			ol.ol_quantity = 1 + rand.nextInt(10)
			ol
		}

		try {
			return atomic { implicit txn =>
				// In WAREHOUSE table: retrieve W_TAX
				val warehouse = Hyflow.dir.open[TpccWarehouse](Name.W(w_id))
				val W_TAX = warehouse.W_TAX()

				// In DISTRICT table: retrieve D_TAX, get and inc D_NEXT_O_ID
				val district = Hyflow.dir.open[TpccDistrict](Name.D(w_id, d_id))
				val D_TAX = district.D_TAX()
				val o_id = district.D_NEXT_O_ID()
				district.D_NEXT_O_ID() = o_id + 1

				// In CUSTOMER table: retrieve discount, last name, credit status 
				val customer = Hyflow.dir.open[TpccCustomer](Name.C(w_id, d_id, c_id))
				val C_DISCOUNT = customer.C_DISCOUNT()
				val C_LAST = customer.C_LAST()
				val C_CREDIT = customer.C_CREDIT()

				// Create entries in ORDER and NEW-ORDER
				val order = new TpccOrder(w_id, d_id, o_id)
				order.O_C_ID() = customer.C_ID
				order.O_CARRIER_ID() = null
				order.O_ALL_LOCAL() = !lines.exists(_.ol_supply_w_id != w_id)
				order.O_OL_CNT() = lines.length

				val newOrder = new TpccNewOrder(w_id, d_id, o_id)

				var i = 1
				while (i <= lines.length) {
					val ol = lines(i-1)
					// For each order line
					val item = Hyflow.dir.open[TpccItem](Name.I(ol.ol_i_id))
					if (item == null) {
						throw new Exception("Rolling back")
					}

					// Retrieve item info
					val I_PRICE = item.I_PRICE()
					val I_NAME = item.I_NAME()
					val I_DATA = item.I_DATA()
					
					// Retrieve stock info
					val stock = Hyflow.dir.open[TpccStock](Name.S(w_id, ol.ol_i_id))
					val S_QUANTITY = stock.S_QUANTITY()
					val S_DIST = d_id match {
						case 1 => stock.S_DIST_01()
						case 2 => stock.S_DIST_02()
						case 3 => stock.S_DIST_03()
						case 4 => stock.S_DIST_04()
						case 5 => stock.S_DIST_05()
						case 6 => stock.S_DIST_06()
						case 7 => stock.S_DIST_07()
						case 8 => stock.S_DIST_08()
						case 9 => stock.S_DIST_09()
						case 10 => stock.S_DIST_10()
					}
					val S_DATA = stock.S_DATA()
					if (S_QUANTITY - 10 > ol.ol_quantity) {
						stock.S_QUANTITY() = S_QUANTITY - ol.ol_quantity
					} else {
						stock.S_QUANTITY() = S_QUANTITY - ol.ol_quantity + 91
					}
					stock.S_YTD() = stock.S_YTD() + ol.ol_quantity
					stock.S_ORDER_CNT() = stock.S_ORDER_CNT() + 1
					// TODO: If line is remote, inc stock.S_REMOTE_CNT()
					
					val orderLine = new TpccOrderLine(w_id, d_id, o_id, i)
					orderLine.OL_QUANTITY() = ol.ol_quantity
					orderLine.OL_I_ID() = item.I_ID
					orderLine.OL_SUPPLY_W_ID() = w_id // TODO: what is the supply warehouse for remote orders?
					orderLine.OL_AMOUNT() = ol.ol_quantity * I_PRICE
					// TODO: examine strings for ORIGINAL, set brand-generic field
					orderLine.OL_DELIVERY_D() = null
					orderLine.OL_DIST_INFO() = S_DIST
					
					ol.orderLine = orderLine
					i += 1
				}
				val totalAmount = lines.map(_.orderLine.OL_AMOUNT()).sum
				totalAmount
			}
		} catch {
			case e: Exception => 0.0
		}
	}

	def payment() = {
		
		// Data Generation
		val w_id = 1
		val d_id = rand.nextInt(10) + 1
		//val c_id = NURand(1023, 1, 3000)
		val c_id = //NURand(103, 1, TpccInit.NUM_CUSTOMERS_PER_D)
					1+rand.nextInt(TpccInit.NUM_CUSTOMERS_PER_D)
		val c_name = TpccInit.genName(NURand(255,0,999))
		val selectByName = rand.nextInt(100) <= 60
		
		var c_w_id = w_id
		var c_d_id = d_id
		if (rand.nextInt(100) > 85) {
			// Select client from remote warehouse!
			// TODO
			
		}
		val h_amount = rand.nextInt(500000) * 0.01
		
		try {
			atomic { implicit txn => 
				// In WAREHOUSE table
				val warehouse = Hyflow.dir.open[TpccWarehouse](Name.W(w_id))
				warehouse.W_YTD() += h_amount

				// In DISTRICT table
				val district = Hyflow.dir.open[TpccDistrict](Name.D(w_id, d_id))
				district.D_YTD() += h_amount
				
				// In CUSTOMER table
				val customer = if (true /* selectByName */) {
					// select by id
					Hyflow.dir.open[TpccCustomer](Name.C(w_id, d_id, c_id))
				} else {
					// TODO: select by name
					null
				}
				
				customer.C_BALANCE() -= h_amount
				customer.C_YTD_PAYMENT() += h_amount
				customer.C_PAYMENT_CNT() += 1
				if (customer.C_CREDIT() == "BC") {
					var c_data = customer.C_DATA()
					c_data = "%s,%s,%s,%s,%s,%s|%s".format(c_id, c_d_id, c_w_id, d_id, w_id, h_amount, c_data)
					if (c_data.length > 500)
						c_data = c_data.splitAt(500)._1
					customer.C_DATA() = c_data
				}
				
				val hist = new TpccHistory("history-"+rand.nextInt().toHexString)
				hist.H_W_ID() = c_w_id
				hist.H_D_ID() = c_d_id
				hist.H_C_ID() = c_id
				hist.H_DATE() = new java.util.Date().toString()
				hist.H_AMOUNT() = h_amount
				hist.H_DATA() = warehouse.W_NAME() + "    " + district.D_NAME()
				
				( warehouse.W_STREET_1(), warehouse.W_STREET_2(), warehouse.W_CITY(), 
						warehouse.W_STATE(), warehouse.W_ZIP(), district.D_STREET_1(),
						district.D_STREET_2(), district.D_CITY(), district.D_STATE(), 
						district.D_ZIP(), customer.C_FIRST(), customer.C_MIDDLE(), 
						customer.C_LAST(), customer.C_CREDIT(), customer.C_CREDIT_LIM(), 
						customer.C_DISCOUNT(), customer.C_BALANCE(), customer.C_CREDIT(),
						customer.C_DATA(), hist.H_DATE() )
			}
		} catch {
			case e: Exception => null
		}
	}
	
	def orderStatus() = {
		// Need query+ index, not supported yet
		// Data Generation
		val w_id = 1
		val d_id = rand.nextInt(10) + 1
		//val c_id = NURand(1023, 1, 3000)
		val c_id = //NURand(103, 1, TpccInit.NUM_CUSTOMERS_PER_D)
			1+rand.nextInt(TpccInit.NUM_CUSTOMERS_PER_D)
		val c_name = TpccInit.genName(NURand(255,0,999))
		val selectByName = rand.nextInt(100) <= 60
		
		try {
			atomic { implicit txn =>
				// In CUSTOMER table
				val customer = if (true /* selectByName */) {
					// select by id
					Hyflow.dir.open[TpccCustomer](Name.C(w_id, d_id, c_id))
				} else {
					// TODO: select by name
					null
				}
				
				val order = Hyflow.dir.open[TpccOrder](Name.O(w_id, d_id, 1))// Oops :(
				
				//Select all orderlines --> this is inefficient
				var olsum = 0.0
				var i = 1
				while (i <= order.O_OL_CNT()) {
					val orderItem = Hyflow.dir.open[TpccOrderLine](Name.OL(w_id, d_id, order.O_ID, i))
					olsum += orderItem.OL_AMOUNT()
					i += 1
				}
			}
		} catch {
			case e: Exception =>
		}
	}
	
	def delivery() = {
		val w_id = 1
		val o_carrier_id = rand.nextInt(10) + 1

		for (d_id <- 1 to 10) {
			atomic { implicit txn =>
				val shipped = Hyflow.dir.open[TpccAuxShipped](Name.shipped(w_id))
				val o_id = shipped.AS_O_ID() + 1
				val order = Hyflow.dir.open[TpccOrder](Name.O(w_id, d_id, o_id))
				Hyflow.dir.delete(Name.NO(w_id, d_id, o_id))
				order.O_CARRIER_ID() = o_carrier_id.toString()
				
				//Select all orderlines --> this is inefficient
				var olsum = 0.0
				val crtdate = new java.util.Date().toString()
				var i=1
				while (i <= order.O_OL_CNT()) {
					val orderItem = Hyflow.dir.open[TpccOrderLine](Name.OL(w_id, d_id, order.O_ID, i))
					orderItem.OL_DELIVERY_D() = crtdate
					olsum += orderItem.OL_AMOUNT()
					i += 1
				}
				
				val customer = Hyflow.dir.open[TpccCustomer](Name.C(w_id, d_id, order.O_C_ID()))
				customer.C_BALANCE() += olsum
				customer.C_DELIVERY_CNT() += 1
			}
		}
	}
	
	def stockLevel() = {
		val w_id = 1
		val d_id = 1 + rand.nextInt(10)
		val thresh = 10 + rand.nextInt(11)
		
		val d_next_o_id = atomic { implicit txn =>
			val district = Hyflow.dir.open[TpccDistrict](Name.D(w_id, d_id))
			district.D_NEXT_O_ID()
		}
			
		val itemSet = mutable.Set[Int]()
		var break = false
		var i = 1
		while(i <= 20) {
			if (! break) {
				val orderItemSet = atomic { implicit txn =>
					val o_id = d_next_o_id - 21 + i
					val order = Hyflow.dir.open[TpccOrder](Name.O(w_id, d_id, o_id))
					
					if (order != null) {
						var orderItemSet = Set[Int]()
						var j = 1
						while (j <= order.O_OL_CNT()) {
							val ol = Hyflow.dir.open[TpccOrderLine](Name.OL(w_id, d_id, o_id, j))
							orderItemSet += ol.OL_I_ID()
							j += 1
						}
						orderItemSet
					} else
						null
					
				}
				if (orderItemSet != null) {
					itemSet ++= orderItemSet
				} else {
					break = true
				} 
			}
			i += 1
		}
		
		val itemList = itemSet.toList
		var cnt = 0
		i = 1
		while(i <= itemList.length/10) {
			val partialCount = atomic { implicit txn =>
				var res = 0
				var j = 10*i
				while (j <= 10*(i+1)-1) {
					if (j < itemList.length) {
						val stock = Hyflow.dir.open[TpccStock](Name.S(w_id, itemList(j)))
						if (stock.S_QUANTITY() < thresh) {
							res += 1
						}
					}
					j += 1
				}
				res
			}
			cnt += partialCount
			i += 1
		}
		cnt
	}
}
