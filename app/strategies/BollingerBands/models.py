from pydantic import BaseModel, Field


class BollingerBands(BaseModel):
    """
    Bollinger Bands Configuration

    days_back_to_consider: The number of days back to consider in calculation
    num_std: The number of std to define the width of the Bollinger bands
    """

    days_back_to_consider: int = Field(30, g=0)
    num_std: int = Field(2, ge=1, le=3)

