
MERGE INTO public.dim_date AS source 
USING public.stage_date AS stage
    ON source.date = stage.date
WHEN NOT MATCHED THEN
    INSERT (date, year, month, dayofweek) 
    VALUES (stage.date, stage.year, stage.month, stage.dayofweek)
WHEN MATCHED THEN 
    UPDATE SET 
    year = stage.year, 
    month = stage.month, 
    dayofweek = stage.dayofweek;


MERGE INTO public.fact_stock_price AS source 
USING public.stage_stock AS stage
    ON source.date = stage.date AND source.ticker_id = stage.ticker_id
WHEN NOT MATCHED THEN
    INSERT (date, ticker_id, open, high, low, close, volume) 
    VALUES (stage.date, stage.ticker_id, stage.open, stage.high, stage.low, stage.close, stage.volume)
WHEN MATCHED THEN 
    UPDATE SET 
    open = stage.open, 
    high = stage.high, 
    low = stage.low, 
    close = stage.close, 
    volume= stage.volume;