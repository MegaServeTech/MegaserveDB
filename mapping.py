import re
from datetime import datetime

def normalize_column_name(column_name, column_mapping):
    """Normalize column names by keeping only alphabetic characters and ignoring case."""
    # Convert to lowercase and keep only alphabetic characters
    column_name_cleaned = re.sub(r'[^a-z]', '', str(column_name).lower())
    
    for standard_name, info in column_mapping.items():
        # Normalize each alias: keep only alphabetic characters, lowercase
        aliases_cleaned = [re.sub(r'[^a-z]', '', alias.lower()) for alias in info["aliases"]]
        
        # Check if the cleaned column name matches any cleaned alias
        if column_name_cleaned in aliases_cleaned:
            return standard_name
    return column_name

# Existing column mappings (unchanged)
portfolios_column_mapping = {
    "portfolio_name": {"aliases": ["portfolio name", "Portfolio_Name", "PORTFOLIO NAME", "portfolioname"], "datatype": "VARCHAR(255)"},
    "user_id": {"aliases": ["user id", "userid", "user_id", "USERID", "UserID", "User ID", "USER ID"], "datatype": "VARCHAR(255)"},
    "user_alias": {"aliases": ["user alias", "User_Alias", "USER ALIAS", "useralias", "UserAlias"], "datatype": "VARCHAR(255)"},
    "pnl": {"aliases": ["pnl", "PNL", "Pnl"], "datatype": "FLOAT"},
    "pnl_per_lot": {"aliases": ["pnl per lot", "PNL_Per_Lot", "PNL PER LOT", "pnlperlot"], "datatype": "FLOAT"},
    "ce_pnl": {"aliases": ["ce pnl", "CE_PNL", "CE PNL", "cepnl"], "datatype": "FLOAT"},
    "pe_pnl": {"aliases": ["pe pnl", "PE_PNL", "PE PNL", "pepnl"], "datatype": "FLOAT"},
    "max_pnl": {"aliases": ["max pnl", "Max_PNL", "MAX PNL", "maxpnl"], "datatype": "FLOAT"},
    "min_pnl": {"aliases": ["min pnl", "Min_PNL", "MIN PNL", "minpnl"], "datatype": "FLOAT"},
    "underlying_price": {"aliases": ["underlying price", "Underlying_Price", "UNDERLYING PRICE", "underlyingprice"], "datatype": "FLOAT"},
    "server": {"aliases": ["server", "Server", "SERVER"], "datatype": "VARCHAR(255)"},
    "date": {"aliases": ["date", "Date", "DATE"], "datatype": "DATE"},
    "exchange": {"aliases": ["exchange symabol", "Exchange Symbol", "EXCHANGESYBAMOL"], "datatype": "VARCHAR(255)"},
    "strategy_tag": {"aliases": ["strategy tag", "Strategy_Tag", "STRATEGY TAG", "strategytag"], "datatype": "VARCHAR(255)"},
    "status": {"aliases": ["status", "Status", "STATUS"], "datatype": "VARCHAR(255)"},
    "sqoff_done": {"aliases": ["sqoff done", "SqOff_Done", "SQOFF DONE", "sqoffdone"], "datatype": "VARCHAR(255)"},
    "total_legs": {"aliases": ["total legs", "Total_Legs", "TOTAL LEGS", "totallegs"], "datatype": "INT"},
    "total_orders": {"aliases": ["total orders", "Total_Orders", "TOTAL ORDERS", "totalorders"], "datatype": "INT"},
    "total_lots": {"aliases": ["total lots", "Total_Lots", "TOTAL LOTS", "totallots"], "datatype": "INT"},
    "max_pnl_time": {"aliases": ["max pnl time", "Max_PNL_Time", "MAX PNL TIME", "maxpnltime"], "datatype": "DATETIME"},
    "min_pnl_time": {"aliases": ["min pnl time", "Min_PNL_Time", "MIN PNL TIME", "minpnltime"], "datatype": "DATETIME"},
    "sno": {"aliases": ["sno", "SNO", "Sno"], "datatype": "INT"},
}

ob_column_mapping = {
    
    "user_id": {"aliases": ["user id", "User_ID", "USER ID", "userid"], "datatype": "VARCHAR(255)"},
    "user_alias": {"aliases": ["user alias", "User_Alias", "USER ALIAS", "useralias"], "datatype": "VARCHAR(255)"},
    "symbol": {"aliases": ["symbol", "Symbol", "SYMBOL"], "datatype": "VARCHAR(255)"},
    "exchange": {"aliases": ["exchange", "Exchange", "EXCHANGE"], "datatype": "VARCHAR(255)"},
    "price": {"aliases": ["price", "Price", "PRICE"], "datatype": "FLOAT"},
    "avg_price": {"aliases": ["avg price", "Avg_Price", "AVG PRICE", "avgprice"], "datatype": "FLOAT"},
    "trigger_price": {"aliases": ["trigger price", "Trigger_Price", "TRIGGER PRICE", "triggerprice"], "datatype": "FLOAT"},
    "server": {"aliases": ["server", "Server", "SERVER"], "datatype": "VARCHAR(255)"},
    "date": {"aliases": ["date", "Date", "DATE"], "datatype": "DATE"},
    "order_time": {"aliases": ["order time", "Order_Time", "ORDER TIME", "ordertime"], "datatype": "VARCHAR(255)"},
    "exchange_time": {"aliases": ["exchange time", "Exchange_Time", "EXCHANGE TIME", "exchangetime"], "datatype": "VARCHAR(255)"},
    "exchg_order_id": {"aliases": ["exchg order id", "Exchg_Order_ID", "EXCHG ORDER ID", "exchgorderid"], "datatype": "VARCHAR(255)"},
    "transaction": {"aliases": ["transaction", "Transaction", "TRANSACTION"], "datatype": "VARCHAR(255)"},
    "order_type": {"aliases": ["order type", "Order_Type", "ORDER TYPE", "ordertype"], "datatype": "VARCHAR(255)"},
    "product": {"aliases": ["product", "Product", "PRODUCT"], "datatype": "VARCHAR(255)"},
    "validity": {"aliases": ["validity", "Validity", "VALIDITY"], "datatype": "VARCHAR(255)"},
    "status": {"aliases": ["status", "Status", "STATUS"], "datatype": "VARCHAR(255)"},
    "quantity": {"aliases": ["quantity", "Quantity", "QUANTITY"], "datatype": "INT"},
    "order_id": {"aliases": ["order id", "Order_ID", "ORDER ID", "orderid"], "datatype": "VARCHAR(255)"},
    "tag": {"aliases": ["tag", "Tag", "TAG"], "datatype": "VARCHAR(255)"},
    "status_message": {"aliases": ["status message", "Status_Message", "STATUS MESSAGE", "statusmessage"], "datatype": "TEXT"},
    "sno": {"aliases": ["sno", "SNO", "Sno"], "datatype": "INT"},
}

user_column_variations = {
    "user_id": {"aliases": ["user id", "userid", "user_id", "USERID", "UserID"], "datatype": "VARCHAR(255)"},
    "alias": {"aliases": ["alias"], "datatype": "VARCHAR(255)"},
    "mtm_all": {"aliases": ["mtm (all)", "MTM_(All)"], "datatype": "FLOAT"},
    "allocation": {"aliases": ["allocation","alocation", "ALOOCATION"], "datatype": "FLOAT"},
    "max_loss": {"aliases": ["maxloss", "max-loss", "max loss"], "datatype": "FLOAT"},
    "server": {"aliases": ["server", "Server", "SERVER"], "datatype": "VARCHAR(255)"},
    "date": {"aliases": ["date", "Date", "DATE"], "datatype": "DATE"},
    "broker": {"aliases": ["broker"], "datatype": "VARCHAR(255)"},
    "algo": {"aliases": ["algo", "ALGO"], "datatype": "VARCHAR(255)"},
    "operator": {"aliases": ["operator", "OPRATOR", "OPERATOR", "Operator"], "datatype": "VARCHAR(255)"},
    "sqoff_done": {"aliases": ["sqoff done"], "datatype": "TEXT"},
    "enabled": {"aliases": ["enabled"], "datatype": "VARCHAR(255)"},
    "logged_in": {"aliases": ["logged in", "loggedin"], "datatype": "VARCHAR(255)"},
    "qty_multiplier": {"aliases": ["qty multiplier"], "datatype": "FLOAT"},
    "available_margin": {"aliases": ["available margin"], "datatype": "FLOAT"},
    "total_orders": {"aliases": ["total orders"], "datatype": "INT"},
    "total_lots": {"aliases": ["total lots"], "datatype": "INT"},
    "remark": {"aliases": ["remark", "remarks", "REMARK", "REMARKS", "Remark"], "datatype": "VARCHAR(255)"},
    "dte": {"aliases": ["DTE","expiry","expiray", "Days to Expiry"], "datatype": "VARCHAR(255)"},
    "sno": {"aliases": ["SNO.", "SNO"], "datatype": "INT"},
}



order_book_column_variations = {
    
    "user_id": {"aliases": ["user id", "userid", "user_id", "USERID", "UserId"], "datatype": "VARCHAR(255)"},
    "user_alias": {"aliases": ["user alias", "UserAlias", "USER ALIAS"], "datatype": "VARCHAR(255)"},
    "symbol": {"aliases": ["symbol", "TradingSymbol", "SYMBOL"], "datatype": "VARCHAR(255)"},
    "exchange": {"aliases": ["exchange", "exch", "Exchange"], "datatype": "VARCHAR(255)"},
    "avg_price": {"aliases": ["avg price", "OrderAverageTradedPrice", "AveragePrice", "AVG PRICE"], "datatype": "FLOAT"},
    "product": {"aliases": ["product", "ProductType", "PRODUCT"], "datatype": "VARCHAR(255)"},
    "status": {"aliases": ["status", "OrderStatus", "STATUS"], "datatype": "VARCHAR(255)"},
    "quantity": {"aliases": ["quantity", "OrderQuantity", "QTY", "order_quantity"], "datatype": "INT"},
    "server": {"aliases": ["server", "Server", "SERVER"], "datatype": "VARCHAR(255)"},
    "date": {"aliases": ["date", "Date", "DATE"], "datatype": "DATE"},
    "order_time": {"aliases": ["order time", "OrderGeneratedDateTime", "ORDER TIME"], "datatype": "DATETIME"},
    "exchg_order_time": {"aliases": ["exchg order time", "ExchangeTransactTime", "EXCH ORDER TIME"], "datatype": "DATETIME"},
    "exchg_order_id": {"aliases": ["exchg order id", "ExchangeOrderID", "EXCH ORDER ID"], "datatype": "VARCHAR(255)"},
    "transaction": {"aliases": ["transaction", "OrderSide", "TRANSACTION", "trantype"], "datatype": "VARCHAR(255)"},
    "order_type": {"aliases": ["order type", "OrderType", "ORDER TYPE"], "datatype": "VARCHAR(255)"},
    "limit_price": {"aliases": ["limit price", "OrderPrice", "LIMIT PRICE"], "datatype": "FLOAT"},
    "trigger_price": {"aliases": ["trigger price", "OrderStopPrice", "TRIGGER PRICE"], "datatype": "FLOAT"},
    
    "filled_quantity": {"aliases": ["filled quantity", "CumulativeQuantity", "FILL QUANTITY"], "datatype": "INT"},
    "tag": {"aliases": ["tag", "Tags", "TAGS"], "datatype": "VARCHAR(255)"},
    "order_id": {"aliases": ["order id", "AppOrderID", "ORDER ID"], "datatype": "VARCHAR(255)"},
    "error_msg": {"aliases": ["error msg", "CancelRejectReason", "ERROR MESSAGE"], "datatype": "VARCHAR(255)"},
    "sno": {"aliases": ["sno", "SNO"], "datatype": "INT"},
}


strategytag_column_mapping = {
    "strategy_tag": {"aliases": ["strategy tag", "Strategy_Tag", "STRATEGY TAG"], "datatype": "VARCHAR(255)"},
    "pnl": {"aliases": ["pnl", "PNL", "PnL"], "datatype": "FLOAT"},
    "trade_value": {"aliases": ["trade value", "Trade_Value", "TRADE VALUE"], "datatype": "FLOAT"},
    "server": {"aliases": ["server", "Server", "SERVER"], "datatype": "VARCHAR(255)"},
    "date": {"aliases": ["date", "Date", "DATE"], "datatype": "DATE"},
    "enabled": {"aliases": ["enabled", "Enabled", "ENABLED"], "datatype": "VARCHAR(255)"},
    "sqoff_done": {"aliases": ["sqoff done", "SqOff Done", "SQOFF DONE"], "datatype": "VARCHAR(255)"},
    "total_portfolios": {"aliases": ["total portfolios", "Total_Portfolios", "TOTAL PORTFOLIOS"], "datatype": "INT"},
    "total_legs": {"aliases": ["total legs", "Total_Legs", "TOTAL LEGS"], "datatype": "INT"},
    "total_orders": {"aliases": ["total orders", "Total_Orders", "TOTAL ORDERS"], "datatype": "INT"},
    "total_lots": {"aliases": ["total lots", "Total_Lots", "TOTAL LOTS"], "datatype": "INT"},
    "sno": {"aliases": ["sno", "SNO", "Sno"], "datatype": "INT"},
}

legs_column_mapping = {
    "user_id": {"aliases": ["user id", "userid", "user_id", "USERID", "UserID"], "datatype": "VARCHAR(255)"},
    "user_alias": {"aliases": ["user alias", "UserAlias", "user_alias", "USER_ALIAS"], "datatype": "VARCHAR(255)"},
    "leg_id": {"aliases": ["leg id", "LegID", "leg_id"], "datatype": "VARCHAR(255)"},
    "portfolio_name": {"aliases": ["portfolio name", "PortfolioName", "portfolio_name"], "datatype": "VARCHAR(255)"},
    "pnl": {"aliases": ["pnl", "PNL", "ProfitAndLoss"], "datatype": "FLOAT"},
    "pnl_per_lot": {"aliases": ["pnl per lot", "PNLPerLot", "pnl_per_lot"], "datatype": "FLOAT"},
    "ltp": {"aliases": ["ltp", "LTP", "LastTradedPrice"], "datatype": "FLOAT"},
    "server": {"aliases": ["server", "Server", "SERVER"], "datatype": "VARCHAR(255)"},
    "date": {"aliases": ["date", "Date", "DATE"], "datatype": "DATE"},
    "exchange": {"aliases": ["exchange symbol", "Exchange Symbol", "EXCHANGE_SYMBOL"], "datatype": "VARCHAR(255)"},  # Fixed typos
    "strategy_tag": {"aliases": ["strategy tag", "StrategyTag", "strategy_tag"], "datatype": "VARCHAR(255)"},
    "txn": {"aliases": ["txn", "Txn", "TXN"], "datatype": "VARCHAR(255)"},
    "entry_time": {"aliases": ["entry time", "EntryTime", "entry_time"], "datatype": "VARCHAR(255)"},
    "exit_time": {"aliases": ["exit time", "ExitTime", "exit_time"], "datatype": "VARCHAR(255)"},
    "status": {"aliases": ["status", "Status", "STATUS"], "datatype": "VARCHAR(255)"},
    "exit_type": {"aliases": ["exit type", "ExitType", "exit_type"], "datatype": "VARCHAR(255)"},
    "leg_sno": {"aliases": ["leg sno", "LegSNO", "leg_sno"], "datatype": "INT"},
    "lots": {"aliases": ["lots", "Lots", "LOTS"], "datatype": "INT"},
    "entry_qty": {"aliases": ["entry qty", "EntryQty", "entry_qty"], "datatype": "INT"},
    "entry_filled_qty": {"aliases": ["entry filled qty", "EntryFilledQty", "entry_filled_qty"], "datatype": "INT"},
    "entry_avg_price": {"aliases": ["entry avg price", "EntryAvgPrice", "entry_avg_price"], "datatype": "FLOAT"},  # Fixed typo in alias
    "exit_qty": {"aliases": ["exit qty", "ExitQty", "exit_qty"], "datatype": "INT"},
    "exit_filled_qty": {"aliases": ["exit filled qty", "ExitFilledQty", "exit_filled_qty"], "datatype": "INT"},
    "exit_avg_price": {"aliases": ["exit avg price", "ExitAvgPrice", "exit_avg_price"], "datatype": "FLOAT"},
    "total_orders": {"aliases": ["total orders", "TotalOrders", "total_orders"], "datatype": "INT"},
    "total_lots": {"aliases": ["total lots", "TotalLots", "total_lots"], "datatype": "INT"},
    "sno": {"aliases": ["sno", "SNO", "SNo", "S.No", "s no"], "datatype": "INT"},
}

multilegorders_column_mapping = {

    "user_id": {"aliases": ["user id", "userid", "user_id", "USERID", "UserID"], "datatype": "VARCHAR(255)"},
    "user_alias": {"aliases": ["user alias", "UserAlias", "user_alias", "USER_ALIAS"], "datatype": "VARCHAR(255)"},
    "order_id": {"aliases": ["order id", "OrderID", "order_id"], "datatype": "VARCHAR(255)"},
    "leg_id": {"aliases": ["leg id", "LegID", "leg_id"], "datatype": "VARCHAR(255)"},
    "portfolio_name": {"aliases": ["portfolio name", "PortfolioName", "portfolio_name"], "datatype": "VARCHAR(255)"},
    "symbol": {"aliases": ["symbol", "Symbol", "SYMBOL"], "datatype": "VARCHAR(255)"},
    "exchange": {"aliases": ["exchange", "Exchange", "EXCHANGE"], "datatype": "VARCHAR(255)"},
    "server": {"aliases": ["server", "Server", "SERVER"], "datatype": "VARCHAR(255)"},
    "date": {"aliases": ["date", "Date", "DATE"], "datatype": "DATE"},
    "avg_price": {"aliases": ["avg price", "AvgPrice", "avg_price"], "datatype": "FLOAT"},
    "limit_price": {"aliases": ["limit price", "LimitPrice", "limit_price"], "datatype": "FLOAT"},
    "trigger_price": {"aliases": ["trigger price", "TriggerPrice", "trigger_price"], "datatype": "FLOAT"},
    "order_time": {"aliases": ["order time", "OrderTime", "order_time"], "datatype": "DATETIME"},
    "exchg_order_time": {"aliases": ["exchg order time", "ExchgOrderTime", "exchg_order_time"], "datatype": "DATETIME"},
    "exchg_order_id": {"aliases": ["exchg order id", "ExchgOrderID", "exchg_order_id"], "datatype": "VARCHAR(255)"},
    "transaction": {"aliases": ["transaction", "Transaction", "TRANSACTION"], "datatype": "VARCHAR(255)"},
    "product": {"aliases": ["product", "Product", "PRODUCT"], "datatype": "VARCHAR(255)"},
    "order_type": {"aliases": ["order type", "OrderType", "order_type"], "datatype": "VARCHAR(255)"},
    "status": {"aliases": ["status", "Status", "STATUS"], "datatype": "VARCHAR(255)"},
    "quantity": {"aliases": ["quantity", "Quantity", "QUANTITY"], "datatype": "INT"},
    "filled_quantity": {"aliases": ["filled quantity", "FilledQuantity", "filled_quantity"], "datatype": "INT"},
    "tag": {"aliases": ["tag", "Tag", "TAG"], "datatype": "VARCHAR(255)"},
    "remarks": {"aliases": ["remarks", "Remarks", "REMARKS"], "datatype": "TEXT"},
    "sno": {"aliases": ["sno", "SNO", "SNo", "S.No", "s no"], "datatype": "INT"},
}


positions_column_mapping = {
    "user_id": {"aliases": ["user id", "userid", "user_id", "USERID", "UserID", "User ID", "USER ID"], "datatype": "VARCHAR(255)"},
    "user_alias": {"aliases": ["user alias", "User_Alias", "USER ALIAS", "useralias", "UserAlias"], "datatype": "VARCHAR(255)"},
    "symbol": {"aliases": ["symbol", "Symbol", "SYMBOL"], "datatype": "VARCHAR(255)"},
    "exchange": {"aliases": ["exchange", "Exchange", "EXCHANGE"], "datatype": "VARCHAR(255)"},
    "pnl": {"aliases": ["pnl", "PNL", "Pnl"], "datatype": "FLOAT"},
    "pnl_percentage": {"aliases": ["pnl percentage", "PNL_Percentage", "PNL PERCENTAGE", "pnlpercentage"], "datatype": "FLOAT"},
    "realized_profit": {"aliases": ["realized profit", "Realized_Profit", "REALIZED PROFIT", "realizedprofit"], "datatype": "FLOAT"},
    "unrealized_profit": {"aliases": ["unrealized profit", "Unrealized_Profit", "UNREALIZED PROFIT", "unrealizedprofit"], "datatype": "FLOAT"},
    "ltp": {"aliases": ["ltp", "LTP", "Last Traded Price"], "datatype": "FLOAT"},
    "server": {"aliases": ["server", "Server", "SERVER"], "datatype": "VARCHAR(255)"},
    "date": {"aliases": ["date", "Date", "DATE"], "datatype": "DATE"},
    "product": {"aliases": ["product", "Product", "PRODUCT"], "datatype": "VARCHAR(255)"},
    "net_qty": {"aliases": ["net qty", "Net_Qty", "NET QTY", "netqty"], "datatype": "INT"},
    "buy_qty": {"aliases": ["buy qty", "Buy_Qty", "BUY QTY", "buyqty"], "datatype": "INT"},
    "buy_avg_price": {"aliases": ["buy avg price", "Buy_Avg_Price", "BUY AVG PRICE", "buyavgprice"], "datatype": "FLOAT"},
    "buy_value": {"aliases": ["buy value", "Buy_Value", "BUY VALUE", "buyvalue"], "datatype": "FLOAT"},
    "sell_qty": {"aliases": ["sell qty", "Sell_Qty", "SELL QTY", "sellqty"], "datatype": "INT"},
    "sell_avg_price": {"aliases": ["sell avg price", "Sell_Avg_Price", "SELL AVG PRICE", "sellavgprice"], "datatype": "FLOAT"},
    "sell_value": {"aliases": ["sell value", "Sell_Value", "SELL VALUE", "sellvalue"], "datatype": "FLOAT"},
    "sno": {"aliases": ["sno", "SNO", "Sno"], "datatype": "INT"},
}

gridlog_column_mapping = {
    "timestamp": {"aliases": ["timestamp", "Timestamp", "TIMESTAMP", "timestam"], "datatype": "TEXT"},
    "user_id": {"aliases": ["user id", "UserID", "USER ID", "userid", "User_Id"], "datatype": "VARCHAR(255)"},
    "strategy_tag": {"aliases": ["strategy tag", "Strategy_Tag", "STRATEGY TAG", "strategytag", "strategy t."], "datatype": "VARCHAR(255)"},
    "option_portfolio": {"aliases": ["option portfolio", "Option_Portfolio", "OPTION PORTFOLIO", "optionportfolio", "option portfolio"], "datatype": "VARCHAR(255)"},
    
    "log_type": {"aliases": ["log type", "Log_Type", "LOG TYPE", "logtype", "log type"], "datatype": "VARCHAR(255)"},
    "message": {"aliases": ["message", "Message", "MESSAGE"], "datatype": "TEXT"},
    "server": {"aliases": ["server", "Server", "SERVER"], "datatype": "VARCHAR(255)"},
    "date": {"aliases": ["date", "Date", "DATE"], "datatype": "DATE"},
}


table_mappings = {
    "users": user_column_variations,
    "orderbook": order_book_column_variations,
    "portfolios": portfolios_column_mapping,
    "ob": ob_column_mapping,
    "strategytags": strategytag_column_mapping,
    "legs": legs_column_mapping,
    "multilegorders": multilegorders_column_mapping,
    "positions": positions_column_mapping,
    "gridlog": gridlog_column_mapping
}