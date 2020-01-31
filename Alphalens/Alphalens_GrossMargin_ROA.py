from quantopian.pipeline.data import factset
from quantopian.pipeline.data import morningstar
from quantopian.pipeline import Pipeline
from quantopian.research import run_pipeline
from alphalens.utils import get_clean_factor_and_forward_returns
from quantopian.pipeline.filters import QTradableStocksUS
from alphalens.tears import create_full_tear_sheet
from quantopian.pipeline.factors import SimpleMovingAverage, CustomFactor, Returns
from quantopian.pipeline.classifiers.morningstar import Sector
from alphalens.performance import mean_information_coefficient
from alphalens.tears import create_returns_tear_sheet
from alphalens.tears import create_information_tear_sheet
from quantopian.pipeline.data import EquityPricing
from quantopian.pipeline.data.morningstar import Fundamentals

def make_pipeline():
    
    base_universe = QTradableStocksUS()
    
    gross_margin = morningstar.operation_ratios.gross_margin.latest
    
    roa = morningstar.operation_ratios.roa.latest
    
    factor_to_analyze = gross_margin.zscore() + roa.zscore()
    
    sector = Sector()
    
    return Pipeline(
        columns = {
            'factor to analyze': factor_to_analyze,
            'sector' : sector
        },
        screen= base_universe & sector.notnull() & factor_to_analyze.notnull()
    )


factor_data = run_pipeline(make_pipeline(), '2012-1-1', '2019-1-1')
pricing_data = get_pricing(factor_data.index.levels[1], '2012-1-1', '2020-6-1', fields='open_price')

sector_labels, sector_labels[-1] = dict(Sector.SECTOR_NAMES), "Unknown"

merged_data = get_clean_factor_and_forward_returns(
    factor=factor_data['factor to analyze'],
    prices=pricing_data,
    quantiles=5,
    groupby=factor_data['sector'],
    groupby_labels=sector_labels,
    binning_by_group=True,
    periods=(198,252)
)# week = 5, month = 21, quarter = 63, year = 252

mean_information_coefficient(merged_data).plot(title="IC Decay")


create_information_tear_sheet(merged_data, by_group=True, group_neutral=True)
create_returns_tear_sheet(merged_data, by_group=True, group_neutral=True)
create_full_tear_sheet(merged_data)