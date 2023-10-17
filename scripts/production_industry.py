import os
import sys
import json
import itertools
import contextlib
import numpy as np
import pandas as pd
from typing import Optional
from pandas import DataFrame
from datetime import datetime

HEADERS_ENG: dict = {
    ("Сектор отрасли производства", ): "production_industry_sector",
    ("Код таможни", ): "custom_code",
    ("Дата регистрации", ): "registration_date",
    ("ИНН отправителя", ): "sender_tin",
    ("Наименование отправителя", ): "sender_name",
    ("Наименование получателя", ): "recipient_name",
    ("ИНН декларанта", ): "declarant_tin",
    ("Наименование декларанта", ): "declarant_name",
    ("Код страны нахождения декларанта", ): "declarant_country_code",
    ("Страна назначения", ): "destination_country",
    ("Признак контейнерных перевозок", ): "container_transportation_sign",
    ("Условие поставки", ): "delivery_condition",
    ("Пункт поставки товара", ): "goods_delivery_point",
    ("Свидетельство СВХ", ): "svh_certificate",
    ("Название станции/склада", ): "station_warehouse_name",
    ("Район склада", ): "warehouse_area",
    ("Город склада", ): "warehouse_city",
    ("Улица склада", ): "warehouse_street",
    ("Наименование и характеристики товаров", ): "goods_description",
    ("G31_13 (Страна происхождения)", "G31_13"): "origin_country",
    ("Кол-во контейнеров", ): "container_count",
    ("Код товара по ТН ВЭД", ): "tnved_code",
    ("Вес брутто, кг", ): "gross_weight_kg",
    ("Вес нетто, кг", ): "net_weight_kg",
    ("ИНН производителя", ): "manufacturer_tin",
    ("Холдинг/компания-учредитель", ): "holding_founder_company",
    ("Комментарий по товарам", ): "goods_comment"
}


DATE_FORMATS: tuple = ("%Y-%m-%d %H:%M:%S", "%d.%m.%Y", "%d.%m.%Y %H:%M:%S", "%d.%m.%Y %H:%M")


class ProductionIndustry(object):
    def __init__(self, input_file_path: str, output_folder: str):
        self.input_file_path: str = input_file_path
        self.output_folder: str = output_folder

    @staticmethod
    def convert_format_date(date: str) -> Optional[str]:
        """
        Convert to a date type.
        """
        for date_format in DATE_FORMATS:
            with contextlib.suppress(ValueError):
                return str(datetime.strptime(date, date_format).date())

    @staticmethod
    def rename_columns(df: DataFrame) -> None:
        """
        Rename of a columns.
        """
        dict_columns_eng: dict = {}
        for column, columns in itertools.product(df.columns, HEADERS_ENG):
            for column_eng in columns:
                column_strip: str = column.strip()
                if column_strip == column_eng.strip():
                    dict_columns_eng[column] = HEADERS_ENG[columns]
        df.rename(columns=dict_columns_eng, inplace=True)

    def change_type_and_values(self, df: DataFrame) -> None:
        """
        Change data types or changing values.
        """
        with contextlib.suppress(Exception):
            df["container_count"] = df["container_count"].astype(int, errors="ignore")
            df["registration_date"] = df["registration_date"].apply(
                lambda x: self.convert_format_date(str(x)) if x else None
            )
            df["gross_weight_kg"] = df["gross_weight_kg"].astype(float, errors="ignore")
            df["net_weight_kg"] = df["net_weight_kg"].astype(float, errors="ignore")

    def add_new_columns(self, df: DataFrame) -> None:
        """
        Add new columns.
        """
        df['original_file_name'] = os.path.basename(self.input_file_path)
        df['original_file_parsed_on'] = str(datetime.now().strftime("%Y-%m-%d %H:%M:%S"))

    def write_to_json(self, parsed_data: list) -> None:
        """
        Write data to json.
        """
        basename: str = os.path.basename(self.input_file_path)
        output_file_path: str = os.path.join(self.output_folder, f'{basename}.json')
        with open(f"{output_file_path}", 'w', encoding='utf-8') as f:
            json.dump(parsed_data, f, ensure_ascii=False, indent=4)

    def main(self) -> None:
        """
        The main function where we read the Excel file and write the file to json.
        """
        df: DataFrame = pd.read_excel(self.input_file_path, dtype=str)
        df = df.dropna(axis=0, how='all')
        self.rename_columns(df)
        df = df.applymap(lambda x: x.strip() if isinstance(x, str) else x)
        self.change_type_and_values(df)
        self.add_new_columns(df)
        df = df.replace({np.nan: None, "NaT": None})
        self.write_to_json(df.to_dict('records'))


if __name__ == "__main__":
    production_industry: ProductionIndustry = ProductionIndustry(sys.argv[1], sys.argv[2])
    production_industry.main()
