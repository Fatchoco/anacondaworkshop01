from pydantic import BaseModel, ConfigDict

class Util(BaseModel):
    model_config = ConfigDict(validate_assignment=True)

    @staticmethod
    def extract_load(fund_name: str, report_date: str, file_pattern: str) -> None:
        print(f"{fund_name} {report_date} {file_pattern}")
