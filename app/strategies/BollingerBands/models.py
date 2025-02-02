from pydantic import BaseModel, Field


class BollingerBandsConfig(BaseModel):
    """
    Bollinger Bands Configuration

    length: The time period to be used in calculating the SMA which creates the base for the Upper and Lower Bands
    num_std: The number of std to define the width of the Bollinger bands
    quantity_limit: The maximum quantity of the instrument to have in the portfolio
    check_interval: The interval in seconds to check for a new prices and for interval recalculation
    stop_loss_percent: The percent from the price to trigger a stop loss
    """

    length: int = Field(20, g=0)
    num_std: int = Field(2, ge=1, le=3)
    quantity_limit: int = Field(0, ge=0)
    check_interval: int = Field(60, g=0)
    stop_loss_percent: float = Field(0.01, ge=0.0, le=1.0)


class Corridor(BaseModel):
    top: float
    bottom: float
