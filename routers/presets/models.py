from enum import Enum
from loguru import logger
from pydantic import BaseModel, Field, field_validator, constr
from constants import (
    EE_HAND_CRANE,
    EE_LABORATORY,
    PRESET_WHEELSTACK,
    PRESET_WHITESPACE,
    PT_BASE_PLATFORM,
    PT_GRID
)


class CellType(str, Enum):
    whitespace = PRESET_WHITESPACE
    wheelStack = PRESET_WHEELSTACK


class PresetType(str, Enum):
    grid = PT_GRID
    basePlatform = PT_BASE_PLATFORM


class ExtraTypes(str, Enum):
    handCrane = EE_HAND_CRANE
    laboratory = EE_LABORATORY


class ExtraElements(BaseModel):
    type: ExtraTypes = Field(...)
    id: str = Field(...)


class PresetData(BaseModel):
    presetName: str = Field(...)
    presetType: PresetType = Field(...)
    rows: int = Field(...,
                      description='Number of rows to use')
    columns: int = Field(...,
                         description='Number of columns to use')
    rowIdentifiers: list[constr(min_length=1, max_length=4)] = Field(
        [], description='Mark rows with provided order. If not provided rows will be marked as 1 -> `rows`'
        )
    columnIdentifiers: list[constr(min_length=1, max_length=4)] = Field(
        [], description='Mark columns with provided order. If not provided columns will be marked as 1 -> `columns`'
        )
    cellTypes: dict[str, dict[str, CellType]] = Field(...,
                                                     description='{rowIdentifier: {columnIdentifier: speficied type of cell}}')
    extra: dict[str, ExtraElements] = Field(...,
                                            description='Extra elements data')


    @field_validator('extra')
    def validate_extra_ids(cls, extra: dict[str, ExtraElements], info):
        unique_ids: set[str] = set()
        for extra_name, extra_model in extra.items():
            extra_data = extra_model.model_dump()
            extra_id: str = extra_data['id']
            if extra_id in unique_ids:
                raise ValueError(
                    f'Extra elements should have unique `id`s | Element: {extra_name} have duplicate id =>  {extra_id}',
                )
            unique_ids.add(extra_id)
        return extra

    @field_validator('presetType', mode='after')
    def validate_preset_type(cls, presetType: str, info):
        if PT_BASE_PLATFORM == presetType:
            logger.error(f'ValidationData: {info}')
            rows: int = info.data.get('rows')
            columns: int = info.data.get('columns')
            if not (1 <= rows < 3 and 1 <= columns < 3):
                raise ValueError(
                    f"When `presetType` is '{PT_BASE_PLATFORM}', both `rows` and `columns` must be in the range of 1 to 3. "
                    f"Got rows={rows}, columns={columns}."
                )
        return presetType

    @field_validator('rowIdentifiers')
    def validate_rows_size(cls, rowIdentifiers: list[str], info):
        rows = info.data.get('rows')
        if not rowIdentifiers:
            return rowIdentifiers
        if len(rowIdentifiers) != rows:
            raise ValueError(f'Specified `rows` size ({rows}) should be equal to the length of `rowIdentifiers` ({len(rowIdentifiers)})')
        return rowIdentifiers
    
    @field_validator('columnIdentifiers')
    def validate_columns_size(cls, columnIdentifiers: list[str], info):
        columns = info.data.get('columns')
        if not columnIdentifiers:
            return columnIdentifiers
        if len(columnIdentifiers) != columns:
            raise ValueError(f'Specified `columns` size ({columns}) should be equal to the length of `columnIdentifiers` ({len(columnIdentifiers)})')
        return columnIdentifiers
