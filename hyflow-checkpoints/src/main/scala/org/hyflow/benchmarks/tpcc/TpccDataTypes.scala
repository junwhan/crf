package org.hyflow.benchmarks.tpcc

import org.hyflow.api._

import java.util.Date

object Name {
	def W(w_id: Int) = 
		"warehouse-w%d".format(w_id)
	def D(w_id: Int, d_id: Int) = 
		"district-w%d-d%d".format(w_id, d_id)
	def C(w_id: Int, d_id: Int, c_id: Int) = 
		"customer-w%d-d%d-c%d".format(w_id, d_id, c_id)
	def O(w_id: Int, d_id: Int, o_id: Int) = 
		"order-w%d-d%d-o%d".format(w_id, d_id, o_id)
	def NO(w_id: Int, d_id: Int, o_id: Int) = 
		"neworder-w%d-d%d-o%d".format(w_id, d_id, o_id)
	def I(i_id: Int) = 
		"item-i%d".format(i_id)
	def S(w_id: Int, i_id: Int) = 
		"stock-w%d-i%d".format(w_id, i_id)
	def OL(w_id: Int, d_id: Int, o_id: Int, ol_id: Int) = 
		"orderline-w%d-d%d-o%d-ol%s".format(w_id, d_id, o_id, ol_id)
	// Aux
	def shipped(w_id: Int) = 
		"aux-shipped-%d".format(w_id)
}

class TpccWarehouse(val W_ID: Int) extends HObj {
	def _id = Name.W(W_ID)
	val W_NAME = field[String](null)
	val W_STREET_1 = field[String](null)
	val W_STREET_2 = field[String](null)
	val W_CITY = field[String](null)
	val W_STATE = field[String](null)
	val W_ZIP = field[String](null)
	val W_TAX = field[Double](0.0)
	val W_YTD = field[Double](0.0)
}

class TpccDistrict(val D_ID: Int, val D_W_ID: Int) extends HObj {
	def _id = Name.D(D_W_ID, D_ID)
	val D_NAME = field[String](null)
	val D_STREET_1 = field[String](null)
	val D_STREET_2 = field[String](null)
	val D_CITY = field[String](null)
	val D_STATE = field[String](null)
	val D_ZIP = field[String](null)
	val D_TAX = field[Double](0.0)
	val D_YTD = field[Double](0.0)
	val D_NEXT_O_ID = field[Int](0)
}

class TpccCustomer(val C_W_ID: Int, val C_D_ID: Int, val C_ID: Int) extends HObj {
	def _id = Name.C(C_W_ID, C_D_ID, C_ID)
	val C_FIRST = field[String](null)
	val C_MIDDLE = field[String](null)
	val C_LAST = field[String](null)
	val C_STREET_1 = field[String](null)
	val C_STREET_2 = field[String](null)
	val C_CITY = field[String](null)
	val C_STATE = field[String](null)
	val C_ZIP = field[String](null)
	val C_PHONE = field[String](null)
	val C_SINCE = field[String](null)
	val C_CREDIT = field[String](null)
	val C_CREDIT_LIM = field[Double](0.0)
	val C_DISCOUNT = field[Double](0.0)
	val C_BALANCE = field[Double](0.0)
	val C_YTD_PAYMENT = field[Double](0.0)
	val C_PAYMENT_CNT = field[Int](0)
	val C_DELIVERY_CNT = field[Int](0)
	val C_DATA = field[String](null)
}

class TpccHistory(override val _id: String) extends HObj {
	// No primary key?
	def H_C_ID = field[Int](0)
	val H_C_D_ID = field[Int](0)
	val H_C_W_ID = field[Int](0)
	val H_D_ID = field[Int](0)
	val H_W_ID = field[Int](0)
	val H_DATE = field[String](null)
	val H_AMOUNT = field[Double](0.0)
	val H_DATA = field[String](null)
}

class TpccNewOrder(val NO_W_ID: Int, val NO_D_ID: Int, val NO_O_ID: Int) extends HObj {
	def _id = Name.NO(NO_W_ID, NO_D_ID, NO_O_ID)
	// TODO: ?
}

class TpccOrder(val O_W_ID: Int, val O_D_ID: Int, val O_ID: Int) extends HObj {
	def _id = Name.O(O_W_ID, O_D_ID, O_ID)
	val O_C_ID = field[Int](0)
	val O_ENTRY_D = field[String](new Date toString)
	val O_CARRIER_ID = field[String](null)
	val O_OL_CNT = field[Int](0)
	val O_ALL_LOCAL = field[Boolean](true)
}

class TpccOrderLine(val OL_W_ID: Int, val OL_D_ID: Int, val OL_O_ID: Int, val OL_NUMBER: Int) extends HObj {
	def _id = Name.OL(OL_W_ID, OL_D_ID, OL_O_ID, OL_NUMBER)
	val OL_I_ID = field(0)
	val OL_SUPPLY_W_ID = field(0)
	val OL_DELIVERY_D = field[String](null)
	val OL_QUANTITY = field[Int](0)
	val OL_AMOUNT = field[Double](0.0)
	val OL_DIST_INFO = field[String](null)
	
	override def toString = "<OrderLine id='%s'>".format(_id)
}

class TpccItem(val I_ID: Int) extends HObj {
	def _id = Name.I(I_ID)
	val I_IM_ID = field[String](null)
	val I_NAME = field[String](null)
	val I_PRICE = field(0.0)
	val I_DATA = field[String](null)
}

class TpccStock(val S_W_ID: Int, val S_I_ID: Int) extends HObj {
	def _id = Name.S(S_W_ID, S_I_ID)
	val S_QUANTITY = field(0)
	val S_DIST_01 = field[String](null)
	val S_DIST_02 = field[String](null)
	val S_DIST_03 = field[String](null)
	val S_DIST_04 = field[String](null)
	val S_DIST_05 = field[String](null)
	val S_DIST_06 = field[String](null)
	val S_DIST_07 = field[String](null)
	val S_DIST_08 = field[String](null)
	val S_DIST_09 = field[String](null)
	val S_DIST_10 = field[String](null)
	val S_YTD = field(0)
	val S_ORDER_CNT = field(0)
	val S_REMOTE_CNT = field(0)
	val S_DATA = field[String](null)
}

class TpccAuxShipped(val AS_W_ID: Int) extends HObj{
	def _id = Name.shipped(AS_W_ID)
	val AS_O_ID = field(0)
}

