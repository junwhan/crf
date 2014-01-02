package org.hyflow.benchmarks.tpcc

import scala.util.Random
import scala.concurrent.stm._
import org.eintr.loglady.Logging

object TpccInit extends Logging {

	// Constants
	val NUM_ITEMS = 1000 // Correct overall # of items: 100,000
	val NUM_WAREHOUSES = 1
	val NUM_DISTRICTS = 10
	val NUM_CUSTOMERS_PER_D = 300
	val NUM_ORDERS_PER_D = 300
	val MAX_CUSTOMER_NAMES = 100
	
	// Utils
	private val rand = new Random
	private val syllables = Array("BAR", "OUGHT", "ABLE", "PRI", 
			"PRES", "ESE", "ANTI", "CALLY", "ATION", "EING") 
	
	def genName(n: Int) = {
		var res = syllables(n % 10)
		val nd = n / 10
		res = syllables(nd % 10) + res
		syllables(nd / 10) + res
	}

	def aString(x: Int, y: Int) = {
		val len = x + rand.nextInt(y - x + 1)
		var res = ""
		(" " * len).map(x => rand.nextPrintableChar())
	}

	def nString(x: Int, y: Int) = {
		val len = x + rand.nextInt(y - x + 1)
		var i = 0
		var res = ""
		while (i < len) {
			res += rand.nextInt(9) + 1
			i += 1
		}
		res
	}
	
	// Functions for populating the DB
	private def item(id: Int) = atomic { implicit txn =>
		val item = new TpccItem(id)
		item.I_IM_ID() = rand.nextInt(10000).toString
		item.I_NAME() = aString(14, 24)
		item.I_PRICE() = rand.nextInt(10000) * 0.01
		item.I_DATA() = if (rand.nextInt(100) < 10) {
			val s0 = aString(26, 50)
			val (s1, s2) = s0.splitAt(rand.nextInt(s0.length - 8))
			s1 + "ORIGINAL" + s2.drop(8)
		} else {
			aString(26, 50)
		}
	}

	private def warehouse(w_id: Int) = atomic { implicit txn =>
		//Aux
		val shipped = new TpccAuxShipped(w_id)
		shipped.AS_O_ID() = 0
		
		val warehouse = new TpccWarehouse(w_id)
		warehouse.W_NAME() = aString(6, 10)
		warehouse.W_STREET_1() = aString(10, 20)
		warehouse.W_STREET_2() = aString(10, 20)
		warehouse.W_CITY() = aString(10, 20)
		warehouse.W_STATE() = aString(2, 2)
		warehouse.W_ZIP() = nString(1, 4) + "11111"
		warehouse.W_TAX() = rand.nextInt(2000) * 0.0001
		warehouse.W_YTD() = 300000.0
		log.debug("Creating warehouse with id: %s", warehouse._id)
	}

	private def stock(w_id: Int, i_id: Int) = atomic { implicit txn =>
		val stock = new TpccStock(w_id, i_id)
		stock.S_QUANTITY() = 10 + rand.nextInt(91)
		stock.S_DIST_01() = aString(24, 24)
		stock.S_DIST_02() = aString(24, 24)
		stock.S_DIST_03() = aString(24, 24)
		stock.S_DIST_04() = aString(24, 24)
		stock.S_DIST_05() = aString(24, 24)
		stock.S_DIST_06() = aString(24, 24)
		stock.S_DIST_07() = aString(24, 24)
		stock.S_DIST_08() = aString(24, 24)
		stock.S_DIST_09() = aString(24, 24)
		stock.S_DIST_10() = aString(24, 24)
		stock.S_YTD() = 0
		stock.S_ORDER_CNT() = 0
		stock.S_REMOTE_CNT() = 0
		stock.S_DATA() = if (rand.nextInt(100) < 10) {
			val s0 = aString(26, 50)
			val (s1, s2) = s0.splitAt(rand.nextInt(s0.length - 8))
			s1 + "ORIGINAL" + s2.drop(8)
		} else {
			aString(26, 50)
		}
	}
	
	private def district(w_id: Int, d_id: Int) = atomic { implicit txn =>
		val dist = new TpccDistrict(d_id, w_id)
		dist.D_NAME() = aString(6, 10)
		dist.D_STREET_1() = aString(10, 20)
		dist.D_STREET_2() = aString(10, 20)
		dist.D_CITY() = aString(10, 20)
		dist.D_STATE() = aString(2, 2)
		dist.D_ZIP() = nString(1, 4) + "11111"
		dist.D_TAX() = rand.nextInt(2000) * 0.0001
		dist.D_YTD() = 30000.0
		dist.D_NEXT_O_ID() = 3001
	}
	
	private def customer(w_id: Int, d_id: Int, c_id: Int, lastNameNbr: Int) = atomic { implicit txn =>
		val cust = new TpccCustomer(w_id, d_id, c_id)
		cust.C_LAST() = genName(lastNameNbr)
		cust.C_MIDDLE() = "OE"
		cust.C_FIRST() = aString(8, 16)
		cust.C_STREET_1() = aString(10, 20)
		cust.C_STREET_2() = aString(10, 20)
		cust.C_CITY() = aString(10, 20)
		cust.C_STATE() = aString(2, 2)
		cust.C_ZIP() = nString(1, 4) + "11111"
		cust.C_PHONE() = nString(16, 16)
		cust.C_SINCE() = new java.util.Date().toString()
		cust.C_CREDIT() = if (rand.nextInt(100) < 90) "GC" else "BC"
		cust.C_CREDIT_LIM() = 50000.0
		cust.C_DISCOUNT() = rand.nextInt(5000) * 0.0001
		cust.C_BALANCE() = -10.0
		cust.C_YTD_PAYMENT() = 10.0
		cust.C_PAYMENT_CNT() = 1
		cust.C_DELIVERY_CNT() = 0
		cust.C_DATA() = aString(300, 500)
	}
	
	private def history(w_id: Int, d_id: Int, c_id: Int) = atomic { implicit txn =>
		val hist = new TpccHistory("history-"+rand.nextInt().toHexString)
		hist.H_W_ID() = w_id
		hist.H_D_ID() = d_id
		hist.H_C_ID() = c_id
		hist.H_DATE() = new java.util.Date().toString()
		hist.H_AMOUNT() = 10.0
		hist.H_DATA() = aString(12, 24)
	}
	
	private def order(w_id: Int, d_id: Int, o_id: Int, c_id: Int) = atomic { implicit txn =>
		val order = new TpccOrder(w_id, d_id, o_id)
		order.O_C_ID() = c_id
		order.O_ENTRY_D() = new java.util.Date().toString()
		order.O_CARRIER_ID() = if (o_id < 2101) (1 + rand.nextInt(10)).toString else null
		order.O_OL_CNT() = 5 + rand.nextInt(11)
		order.O_ALL_LOCAL() = true
		(order.O_OL_CNT(), order.O_ENTRY_D())
	}
	
	private def orderLine(w_id: Int, d_id: Int, o_id: Int, o_nbr: Int, o_entry_d: String) = atomic { implicit txn =>
		val ol = new TpccOrderLine(w_id, d_id, o_id, o_nbr)
		ol.OL_I_ID() = 1 + rand.nextInt(100000)
		ol.OL_SUPPLY_W_ID() = w_id
		ol.OL_DELIVERY_D() = if (ol.OL_O_ID < 2101) o_entry_d else null
		ol.OL_QUANTITY() = 5
		ol.OL_AMOUNT() = if (ol.OL_O_ID < 2101) 0 else { 0.01 * (1 + rand.nextInt(999999)) }
		ol.OL_DIST_INFO() = aString(24, 24)
	}
	
	private def newOrder(w_id: Int, d_id: Int, o_id: Int) = atomic { implicit txn =>
		val no = new TpccNewOrder(w_id, d_id, o_id)
	}
	
	// Main function
	def populateAll() {
		
		for (i_id <- 1 to NUM_ITEMS) {
			item(i_id)
		}
		
		for (w_id <- 1 to NUM_WAREHOUSES) {
			warehouse(w_id)
			
			for (i_id <- 1 to NUM_ITEMS) {
				stock(w_id, i_id)
			}
			
			for (d_id <- 1 to NUM_DISTRICTS) {
				district(w_id, d_id)
				
				for (c_id <- 1 to NUM_CUSTOMERS_PER_D) {
					val nName = if (c_id <= MAX_CUSTOMER_NAMES) 
							c_id-1
						else
							//TpccOps.NURand(255, 0, 999)
							//TpccOps.NURand(25, 0, MAX_CUSTOMER_NAMES-1)
							rand.nextInt(MAX_CUSTOMER_NAMES)
					customer(w_id, d_id, c_id, nName)
					history(w_id, d_id, c_id)
				}
				
				val customerPerm = rand.shuffle(1 to NUM_CUSTOMERS_PER_D toList)
				for (o_id <- 1 to NUM_ORDERS_PER_D) { 
					val (ol_num, o_entry_d) = order(w_id, d_id, o_id, customerPerm(o_id-1))
					for (n <- 1 to ol_num) {
						orderLine(w_id, d_id, o_id, n, o_entry_d)
						//if (n >= 2101) {
						if (n >= 201) {
							newOrder(w_id, d_id, o_id)
						}
					}
				}
			}
		}
	}

}