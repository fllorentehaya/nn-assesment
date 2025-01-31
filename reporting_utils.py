from datetime import datetime
class ReportingConfig():
    def __init__():
        pass
    def get_date_for_file(self,type_report):
        date_now=datetime.now()
        date_now_formatted=date_now.strftime("%m%d%Y%H%M%S")
        return f"reports/report_{type_report}_{date_now_formatted}.html"