'''
Backtest que engloba empresas con las siguientes características:
- ROIC alto
- FCF Growth alto
- Todos los sectores
- Leverage no mayor al 1,1
- US Stocks
- Rotación cada 6 meses
'''

# Se importan las librerias necesarias#
from quantopian.algorithm import attach_pipeline, pipeline_output                                                         
from quantopian.pipeline import Pipeline
from quantopian.pipeline.data import EquityPricing
from quantopian.pipeline.data import morningstar
from quantopian.pipeline.data import Fundamentals
from quantopian.pipeline.filters.morningstar import Q3000US
from quantopian.pipeline.data.factset import Fundamentals as Factset
from quantopian.pipeline.classifiers.morningstar import Sector
from quantopian.pipeline.data.builtin import USEquityPricing
from quantopian.pipeline.factors import SimpleMovingAverage, CustomFactor, Returns
from quantopian.pipeline.filters import QTradableStocksUS
import quantopian.optimize as opt
 
 
 
#En esta sección inicializamos el backtest#
def initialize(context):

	'''
    Una vez se inicie el backtest esta acción se deberá repetir dos veces al año (31 de Enero y 31 de Julio), para ello llamamos a la función 'rebalance'
	La función Rebalance es custom, ya que únicamente existen funciones para hacer rebalanceos de forma diaria, semanal y mensual.
	Personalmente esta es la parte que menos me gusta de la inversión quant y es que se centra en periodos excesivamente cortos de tiempo.
	Para aquellos que os habéis leido la biografía de Jim Simons (para mi el primer quant), empezó haciendo gestión de materias primas a 'largo plazo' y terminó haciendoló intradia
	'''
    schedule_function(rebalance, 
                      date_rules.month_end(), 
                      time_rules.market_close())
     
    #Llamamos a nuestro propio selector de acciones y lo damos el nombre que queramos.#
    attach_pipeline(make_pipeline(), 'my_pipeline')
	          
#Este sería nuestro screener. El modelo que llamaría el equipo de Adarve#			  
def make_pipeline():
    
    #Nuestro universo de acciones serán todas aquellas que se puedan comprar en US#
    base_universe = QTradableStocksUS()
	
    #Nuestro primer factor será el fcf: Cash Flow Operations minus Capital Expenditures.#
    fcf_growth = Fundamentals.free_cash_flow.latest
 
    #Segundo factor será el ROIC: Net Income / (Total Equity + Long-term Debt and Capital Lease Obligation + Short-term Debt and Capital Lease Obligation)#
    roic = morningstar.operation_ratios.roic.latest
    
    #Filtramos por aquellas con Roic más alto (he estado jugando con los percentiles por lo que quizas el rto no sea el mismo)#
    high_roic = roic.percentile_between(60,100, mask=base_universe)
    
    #Filtramos por aquellas con FCF que más ha crecido#
    high_fcfgrowth = fcf_growth.percentile_between(60, 100, mask = base_universe)
    
    #Mezclamos los dos anteriores y hacemos un único ranking#
    securities_of_high_growth = (high_roic & high_fcfgrowth)
    
    #De ese ranking queremos que nos saque los siguientes datos#
    return Pipeline(
        columns = {
            'Return on Invested Capital': roic,
            'Top Growth Company': securities_of_high_growth, 
            'FCF Growth': fcf_growth,
            'Sector': Sector(),
        },
        screen = securities_of_high_growth
    )
	
	#Debe devolver a la función anterior el Pipeline#
    return Pipeline()
 
#Antes del inicio de cada sesión se seleccionana las posiciones que conforman el indice#
def before_trading_start(context, data):
    context.output = pipeline_output('my_pipeline')
    
    #Filtramos por el indice creado en el pipeline#
    context.longs = context.output[context.output['Top Growth Company']].index
    
    #Llamamos a la función que asigna el peso de cada una de las posiciones#
    context.long_weight = assign_weights(context,data)
  
#Asignamos el peso de cada posición con esta función#  
def assign_weights(context,data):

    #Iniciamos un diccionario de pesos de las posiciones#
    weights = {}
    #Calculamos el peso de cada una de ellas, con un leverage de 1,1 y sin exceder el 5% de la cartera#
    if len(context.longs) > 0:
        long_weight = 1.1 / len(context.longs)
		if long_weight > 0.05:
			long_weight = 0.05
		log.info(long_weight)
	else:
		return weights
	#Sacamos todas esas posiciones si no están en el listado#
    for security in context.portfolio.positions:
        if security not in context.longs and data.can_trade(security):
            weights[security] = 0

    for security in context.longs:
        weights[security] = long_weight
	#Devolvemos los pesos#	
	return weights

#Función custom para rebalancear en enero y julio a final de mes#	
def rebalance(context,data):
	today = get_datetime()  
    if today.month in [1]:  

		target_weights = assign_weights(context,data)
 
		if target_weights:
			#Realizamos las ordenes#
			order_optimal_portfolio(
				objective=opt.TargetWeights(target_weights),
				constraints=[],
			)
        
		
	if today.month in [7]:  
		target_weights = assign_weights(context, data)
 
		if target_weights:
			order_optimal_portfolio(
				objective=opt.TargetWeights(target_weights),
				constraints=[],
			)
	
	return