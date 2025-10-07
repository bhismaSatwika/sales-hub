import os

rootWeb = os.path.dirname(os.path.dirname(__file__))

loc = {
    "file_inventory_submit": rootWeb + "/files/inventory_submit/",
    "file_inventory_transfer": rootWeb + "/files/inventory_transfer/",
    "file_inventory_delivery_order": rootWeb + "/files/inventory_delivery_order/",
    "file_inventory_sales_order": rootWeb + "/files/inventory_sales_order/",
    "file_inventory_receipt_transfer": rootWeb + "/files/inventory_receipt_transfer/",
    "file_invoice_sales_order": rootWeb + "/files/invoice_sales_order/",
    "file_invoice_delivery_order": rootWeb + "/files/invoice_delivery_order/",
    "file_sales_recap_report": rootWeb + "/files/sales_recap_report/",
    "file_invoice": rootWeb + "/files/invoice/",
    "file_template_download": rootWeb + "/files/template_download/",
}


par = {}
