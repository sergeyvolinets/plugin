export function onBeforeCalculate(quote, lines, conn) 
{
	var ret = null;

	if (lines.length) 
	{
		var aSchemeLMBQ = new Set();

		var iLowerBound =999999999;
		var iUpperBound = 0;
		var aSchemeLMQ = new Set();
		var aBandLMQ = new Set();

		var aRPA = new Set();
		var aSSP = new Set();
		var qre = quote.record; 
		lines.forEach(function(line) 
		{
			var rec = line.record;
			
			if (rec['Ignore_Price_Rules__c'] != true)
			{
				var iIndex = rec['Quote_Line_Index__c'];
				var sBand = rec['License_Band__c'];
				if(rec['Licence_Metric_Driver__c']==1)
				{
					aSchemeLMBQ.add(iIndex);

					if(sBand != null)
					{
						var iBand = parseInt(sBand);
						if(iBand >iUpperBound)
							iUpperBound = iBand;
						if(iBand <iLowerBound)
							iLowerBound = iBand;
					}
					_debugRec(rec, 'added LMBQ ' +iIndex);
				}
				if(rec['Licence_Metric_Driver__c']==0)
				{
					aSchemeLMQ.add(iIndex);
					aBandLMQ.add(sBand);
					_debugRec(rec, 'added LMQ ' +iIndex);					
				}
			}
			if(rec['Sales_Region__c'] != '' && rec['Sales_Product_Set__c'] != '')
			{
				var sKey = rec['Sales_Product_Set__c']+'__'+rec['Sales_Region__c'] ;
				aRPA.add(sKey);
			}
			if(true)
			{
				aSSP.add(rec['SSPConcate__c']);
			}
		});

		aSchemeLMBQ =  Array.from(aSchemeLMBQ);
		aSchemeLMQ =  Array.from(aSchemeLMQ);	
		_debug('aSchemeLMBQ='+aSchemeLMBQ);
		aBandLMQ =  Array.from(aBandLMQ);
		aRPA = Array.from(aRPA);
		aSSP = Array.from(aSSP);

		_debug('before lookups aSchemeLMBQ: '+aSchemeLMBQ.length+' aSchemeLMQ: '+aSchemeLMQ.length+' aRPA: '+aRPA.length+' aSSP:'+aSSP.length);

		var lmbq=[],lmq=[],ssp=[],rpa=[];
		return queryLMBQ(aSchemeLMBQ,iUpperBound,iLowerBound,conn,lmbq)
			.then(function(){
				return queryLMQ(aSchemeLMQ,aBandLMQ, conn,lmq);
			}).then(function(){
				return querySSP(aSSP, conn,ssp);
			}).then(function(){
				return queryRPA(aRPA, conn,rpa);
			}).then(function(){
				return calc(quote,lines, lmq, lmbq, ssp, rpa,conn);
//			}).then(function(){
//				return SaveExt(quote.record,conn);
//			}).catch(function(error){
//				throw error;
  	//			console.log(error);
			});
	}
	return Promise.resolve();
}

		
	var schemeFields = 
		[
			'Id', 
			'Upper_Bound__c',
			'Lower_Bound__c',
			'Price_Scheme_Index__c',
			'Licence_Metric_Output__c',
			'Recurring_Price_for_Tier__c',
			'Initial_Price_for_Tier__c', 
			'X3rd_party_Cost_Type__c', 
			'Initial_3rd_party_Cost__c',
			'Initial_Cost_Currency__c',
			'Recurring_3rd_Party_Cost__c',
			'Recurring_Unit_Price__c',
			'Initial_Unit_Price__c',
			'Recurring_Cost_Currency__c'
		];

function queryLMBQ(aSchemeLMBQ,iUpperBound,iLowerBound,conn,arr)
{
	var promise = new Promise(function(resolve, reject) 
	{
		if (aSchemeLMBQ.length) //LMBQ Price Rule
		{
			var conditions = 
			{
				Price_Scheme_Index__c: {$in: aSchemeLMBQ},
				Upper_Bound__c : {$gte: iUpperBound},
				Lower_Bound__c: {$lte: iLowerBound}
			};

			conn.sobject('Price_Scheme__c')
			.find(conditions, schemeFields)
			.execute(function(err, records) 
			{
				var mLMBQ = new Map();
				if (err) {				
					return reject(err);
				} else {
						
					records.forEach(function(record) {
						arr.push(record);
						_debug('Price_Scheme__c LMBQ record :'+record.Price_Scheme_Index__c+' '+record.Lower_Bound__c+' '+record.Upper_Bound__c);
					});
					resolve(records);				
				}		
			});
		}
		else
			resolve([]);
	});
	return promise;
}
function queryLMQ(aSchemeLMQ,aBandLMQ, conn,arr)
{
	var promise = new Promise(function(resolve, reject) 
	{
		if (aSchemeLMQ.length) //LMQ Price Rule
		{
			var conditions = 
			{
				Price_Scheme_Index__c: {$in: aSchemeLMQ},
				Licence_Metric_Output__c : {$in: aBandLMQ}
			};

			conn.sobject('Price_Scheme__c')
			.find(conditions, schemeFields)
			.execute(function(err, records) 
			{
				if (err) 
				{				
					reject(err);
				} else 
				{
					_debug('Price_Scheme__c LMQ records:'+records.length);
					records.forEach(function(record) {
						arr.push(record);
					});
					resolve(records);
				}
			});
		}
		else
			resolve([]);
	});
	return promise;
}


function querySSP(aSSP, conn,arr)
{
	var promise = new Promise(function(resolve, reject) 
	{
		if(aSSP.length)
		{
			_debug('aSSP: '+aSSP);
			var aFlds = 
			[
				'Id', 'ConcatIndex__c',
				'ISSPL__c', 'ISSPT__c', 'ISSPU__c', 'RSSPL__c', 'RSSPT__c', 'RSSPU__c'
			];
			var conditions = 
				{
					ConcatIndex__c: {$in: aSSP}
				};

			conn.sobject('Single_Sales_Price__c')
			.find(conditions, aFlds)
			.execute(function(err, records) 
			{
				if (err)
				{
					reject(err);
				}
				else {
//					_debug('SSP records: '+records.length);
					records.forEach(function(record) {
						arr.push(record);
					});
					resolve(records);
				}
			});
		}
		else
			resolve([]);
	});
	return promise;
}
function queryRPA(aRPA, conn,arr)
{
	var promise = new Promise(function(resolve, reject) 
	{
		if(aRPA.length)
		{
			_debug('aRPA: '+aRPA);
			var aFlds = 
			[
				'Id', 
				'Search_Key__c',
				'Initial_Rate__c',
				'Recurring_Rate__c'
			];
			var condRPA = 
				{
					Search_Key__c: {$in: aRPA}
				};

			conn.sobject('Regional_Price_Adjustment__c')
			.find(condRPA, aFlds)
			.execute(function(err, records) 
			{
				if (err) {
					reject(err);
				} else {
					records.forEach(function(record) {
						arr.push(record);
					});
					resolve(records);
				}
			});
		}
		else
			resolve([]);
	});
	return promise;
}


function calc(quote,lines, lmq, lmbq, ssp, rpa,conn)
{
//	_debug('start calc, lmq: '+lmq+', lmbq: '+lmbq+', ssp: '+ssp+',rpa: '+rpa);
	var mSSP = new Map(),
		mRPA = new Map(),
		mLMQ = new Map(),
		mLMBQ = new Map();

	lmbq.forEach(function(record) 
	{
		var iIndex = record.Price_Scheme_Index__c;
		var aVal = mLMBQ.get(iIndex);

		if(aVal == undefined)
		{
			aVal = [];
			mLMBQ.set(iIndex, aVal);
			
		}
		aVal.push(record);
	});		
	lmq.forEach(function(record) {
		var iIndex = record.Price_Scheme_Index__c;
		var aVal = mLMQ.get(iIndex);
		if(aVal == undefined){
			aVal = [];
			mLMQ.set(iIndex, aVal);
		
		}
		aVal.push(record);
	});
	ssp.forEach(function(record) {
		mSSP.set(record.ConcatIndex__c, record);
	});
	rpa.forEach(function(record){
		mRPA.set(record.Search_Key__c, record);
	});

	var qre = quote.record;
	
	if(qre['SBQQ__SubscriptionTerm__c'] == null)
		qre['SBQQ__SubscriptionTerm__c'] = getTerm(qre);

	var InitOfferTarget =0,InitnoTargetAlloc=0;

	lines.forEach(function(line) 
	{
		var rec = line.record;
		if (rec['Ignore_Price_Rules__c'] != true)
		{
//		_debug('initPrice: '+rec['Initial_Pricebook_Price__c']+' recurPrice: '+rec['Recurring_Pricebook_Price__c']);
			var sIndex = rec['Quote_Line_Index__c'];
			var bFoundPrice = false;
			if(mLMBQ != null)
			{
				var aLMBQ = mLMBQ.get(sIndex);
				if(aLMBQ != undefined)
				{
					var iBand = parseInt(rec['License_Band__c']);				
					aLMBQ.forEach(function(record)
					{
						if(iBand>=record.Lower_Bound__c && iBand <=record.Upper_Bound__c)
						{	
							bFoundPrice = true;									
							rec['SBQQ__DefaultSubscriptionTerm__c'] = 60;
							copyPriceFields(line,record);
							_debug('LMBQ: initPrice: '+rec['Initial_Pricebook_Price__c']+' recurPrice: '+rec['Recurring_Pricebook_Price__c']);
							return;
						}
					});		
				}				
			}
			if(mLMQ != null)
			{
				var aLMQ = mLMQ.get(sIndex);
				if(aLMQ != undefined)
				{
					var sBand = rec['License_Band__c'];				
					aLMQ.forEach(function(record)
					{
						if(sBand ===record.Licence_Metric_Output__c)
						{
							bFoundPrice = true;
							rec['SBQQ__DefaultSubscriptionTerm__c'] = rec['Quote_Subscription_Term__c'];
							copyPriceFields(line,record);
							_debug('LMQ: initPrice: '+rec['Initial_Pricebook_Price__c']+' recurPrice: '+rec['Recurring_Pricebook_Price__c']);
							return;
						}
					});		
				}	
			}
			if(bFoundPrice == false && rec['License_Band__c'] != null && rec['Quote_Line_Index__c'] != null && rec['License_Band__c'] != "")
			{
				if(qre['Deal_Type__c'] !='Termination/Read-only')
					throw ('No pricing found for product "'+rec['SBQQ__ProductName__c']+'". Please input correct Licence Metric, Quanity and Selling Type.');
			}	
		}	

		if(	rec['Revenue_Type__c'] === 'OnPremise ILF/RLF') //Perpetual and +20 Years Calculator //11-19 Term Years Calculator
		{
			if(qre['Perpetual__c']==true ||
				qre['SBQQ__SubscriptionTerm__c']>240
				)
			{
				rec['Recurring_Pricebook_Price__c'] = rec['Recurring_Pricebook_Price__c'] *1.4;
				rec['Initial_Pricebook_Price__c'] = rec['Initial_Pricebook_Price__c'] *1.4;
//				_debug('20+ years: initPrice '+rec['Initial_Pricebook_Price__c']+' recurPrice '+rec['Initial_Pricebook_Price__c']);

			}
			else if(qre['SBQQ__SubscriptionTerm__c']>132)
			{
				rec['Recurring_Pricebook_Price__c'] = rec['Recurring_Pricebook_Price__c'] *1.2;
				rec['Initial_Pricebook_Price__c'] = rec['Initial_Pricebook_Price__c'] *1.2;	
	//			_debug('10+ years: initPrice '+rec['Initial_Pricebook_Price__c']+' recurPrice: '+rec['Initial_Pricebook_Price__c']);	
			}
		}
		if(rec['License_Band__c'] != '')
		{
			if (rec['Ignore_Price_Rules__c'] != true)
			{
				if(rec['Licence_Metric_Driver__c'] == 1)//8.Deal Currency LMBQ
				{
					var iBand = nvl(parseInt(rec['License_Band__c']));
					rec['Recurring_Deal_Currency_List_Price__c'] = nvl(rec['Recurring_Pricebook_Unit_Price__c']) *
					(iBand -rec['Lower_Bound__c']) * qre['Price_Conversion_Rate__c'];

					if(rec['Recurring_Pricebook_Price__c'] != null)
						rec['Recurring_Deal_Currency_List_Price__c'] += rec['Recurring_Pricebook_Price__c'];
					
					rec['Initial_Deal_Currency_List_Price__c']=
					nvl(rec['Initial_Pricebook_Unit_Price__c'])*iBand * qre['Price_Conversion_Rate__c'];
					if(rec['Initial_Pricebook_Price__c'] != null)
						rec['Initial_Deal_Currency_List_Price__c']+=rec['Initial_Pricebook_Price__c'];
	//				_debug('8.LMBQ: initDealPrice '+rec['Initial_Deal_Currency_List_Price__c']+' recurDealPrice '+rec['Recurring_Deal_Currency_List_Price__c']);						
				}

				if(rec['Licence_Metric_Driver__c'] == 0)//9.Deal Currency LMQ
				{
					rec['Recurring_Deal_Currency_List_Price__c'] =  	
						rec['Recurring_Pricebook_Price__c']*qre['Price_Conversion_Rate__c'];
					rec['Initial_Deal_Currency_List_Price__c'] = 
						rec['Initial_Pricebook_Price__c']*qre['Price_Conversion_Rate__c'];
				}
	//			_debug('9.LMQ: initDealPrice '+rec['Initial_Deal_Currency_List_Price__c']+' recurDealPrice '+rec['Recurring_Deal_Currency_List_Price__c']);						
			}
		}
		rec['Initial_Deal_Currency_List_Price__c'] = nvl(rec['Initial_Deal_Currency_List_Price__c']);
		rec['Recurring_Deal_Currency_List_Price__c'] = nvl(rec['Recurring_Deal_Currency_List_Price__c']);
	



		if(qre['is_Amendment__c']) //Ammend Price Rule for quantity
		{
			if(rec['SBQQ__PriorQuantity__c'] == null)
				rec['SBQQ__Quantity__c'] =1;
			else 				
				rec['SBQQ__Quantity__c'] +=rec['SBQQ__PriorQuantity__c'] +1;
			_debug('Ammendment qty: '+rec['SBQQ__Quantity__c']);
		}
		//GS Recurring Update
		if(rec['GS_Revenue_Type__c'] != '' && rec['GS_Revenue_Type__c'] != null)
		{
			rec['SBQQ__ListPrice__c'] = rec['Recurring_Pricebook_Price__c'];
			rec['SBQQ__CustomerPrice__c'] = rec['Recurring_Unit_Price__c'];
			rec['SBQQ__NetPrice__c'] = rec['Recurring_Unit_Price__c'];
			_debugRec(rec,'GS: SBQQ__NetPrice__c = Recurring_Unit_Price__c = '+rec['SBQQ__NetPrice__c']);
		}
		//SSP Allocation
	rec['ISSPL__c'] = rec['ISSPT__c'] = rec['ISSPU__c'] = 
	rec['RSSPL__c'] = rec['RSSPT__c'] = 	rec['RSSPU__c'] = null;
		if(mSSP != null)
		{
			var ssp = mSSP.get(rec['SSPConcate__c']);
			if(ssp != null)
			{
				rec['ISSPL__c'] = nvl(ssp.ISSPL__c)/100;
				rec['ISSPT__c'] = ssp.ISSPT__c;
				rec['ISSPU__c'] = nvl(ssp.ISSPU__c)/100;
				rec['RSSPL__c'] = nvl(ssp.RSSPL__c)/100;
				rec['RSSPT__c'] = ssp.RSSPT__c;
				rec['RSSPU__c'] = nvl(ssp.RSSPU__c)/100;
//				_debug('SSP: '+rec['ISSPL__c']+' '+rec['ISSPT__c']+' '+rec['ISSPU__c']+' '+rec['RSSPL__c']);
			}
		}

		if (rec['External_SKU_DH__c'] === '463-28672'){ //Update Early Termination Fee Price
			rec['Initial_Pricebook_Price__c'] = rec['Remaining_Contract_Amount_Total__c'];
//			_debug('Early Termination Fee: '+rec['Initial_Pricebook_Price__c']);
		}

		if(rec['SBQQ__UpgradedSubscription__c'] != null && qre['is_Amendment']== true)//Ammendment Price
		{
			rec['SBQQ__ListPrice__c'] = rec['Recurring_Deal_Currency_List_Price__c'] * rec['Subscription_Uplift_Factor__c'] - rec['Subscription_Net_Price__c'];
			rec['Comm_Man_Price__c'] = rec['Initial_Deal_Currency_List_Price__c'] - rec['Subscription_Sold_Value'];
//			_debug('Ammendment Price: init:'+rec['Comm_Man_Price__c']+' recur:'+rec['SBQQ__ListPrice__c']);
		}
		if (rec['Ignore_Price_Rules__c'] != true)
		{
			//1I. Initial No Discount
			if(	qre['Initial_Discount_Type__c'] == null && rec['Initial_Discount_Type__c'] == null && 
				rec['Initial_Bundle_Pricing_Filter__c'] === 'ND' && rec['Pearl_Item__c'] == false) 
			{
				var dPrice = rec['Initial_Deal_Currency_List_Price__c'];
				rec['Sold_Value__c'] = dPrice;
				rec['ILF_Fair_Value__c'] = dPrice;
				rec['Comm_Man_Price__c'] = dPrice;
				_debugRec(rec,'1I: '+dPrice);
			}

			//1R. Recurring No Discount
			if(	qre['Recurring_Discount_Type__c'] == null && rec['Recurring_Discount_Type__c'] == null && 
				rec['Recurring_Bundle_Pricing_Filter__c'] == 'ND' && rec['Pearl_Item__c'] == false) 
			{
				var dPrice = rec['Recurring_Deal_Currency_List_Price__c'];
				rec['Fair_Value__c'] = dPrice;
				rec['SBQQ__CustomerPrice__c'] = dPrice;
				rec['Recurring_Net_Total__c'] = dPrice;
				rec['SBQQ__NetPrice__c'] = dPrice;
				_debugRec(rec,'1R: '+dPrice);			
			}
		}

		if(rec['Initial_Discount_Type__c'] === 'Target' && rec['Bundle2__c'] != true) //2I. Initial Line Target
		{
			var dDisc = nvl(rec['Initial_Discount__c']);
			rec['Sold_Value__c'] = dDisc;
			rec['ILF_Fair_Value__c'] = dDisc;
			_debugRec(rec,'2I: '+rec['Initial_Discount__c']);				
		}

		if(rec['Recurring_Discount_Type__c'] === 'Target' && rec['Bundle2__c'] != true) //2R. Recurring Line Target
		{
			var dDisc = nvl(rec['Recurring_Discount__c']);
			rec['SBQQ__ListPrice__c'] = dDisc;
			rec['SBQQ__CustomerPrice__c'] = dDisc;
			rec['Fair_Value__c'] = dDisc;
			rec['Recurring_Net_Total__c'] = dDisc;
			_debugRec(rec,'2R: '+dDisc);			
		}

		if(line['Initial_Pricing_Filter__c'] === '7' || line['Initial_Pricing_Filter__c'] === '1')
			InitOfferTarget+=nvl(rec['Initial_Discount__c']);
		if(line['Initial_Pricing_Filter_Target__c'] === '1')
			InitnoTargetAlloc+=rec['Initial_Deal_Currency_List_Price__c'];
	});

	//	Initial Quote Bundle Push
	qre['Initial_Offering_Target_Total__c'] = InitOfferTarget;
	qre['Initial_Total_List_Price_No_Target__c'] = InitnoTargetAlloc;
//	_debug('Initial Quote Bundle Push: '+InitOfferTarget+' '+InitnoTargetAlloc);

	var vUSListPricenoTarget=0, vUSLiveTarget =0, 	vUSListPricenoTargetre =0, 
	vUSLiveTargetre =0, vUSTotalRevIni = 0,vUSTotalRevREC=0, vUSLivePercTotal=0
	,vUSLivePercTotalRe =0, RecListTotal = 0;
	var vGLListPricenoTarget=0, vGLLiveTarget =0, 	vUSListPricenoTargetre =0, 
	vGLLiveTargetre =0, vGLTotalRevIni = 0,vGLTotalRevREC=0, vGLLivePercTotal=0
	,vGLLivePercTotalRe =0,vGLListPricenoTargetre=0;


	lines.forEach(function(line) 
	{
		var rec = line.record;

		if(rec['Initial_Discount_Type__c'] === 'Percentage' &&
			isNow(rec['Initial_Discount_Start_date__c'])
		) //3I. Initial Line Percentage
		{
			var dVal = rec['Initial_Deal_Currency_List_Price__c']*(1-(rec['Initial_Discount__c']/100));
			rec['Sold_Value__c'] = dVal;
			rec['ILF_Fair_Value__c'] = dVal;
			_debugRec(rec,'3I: '+dVal);
		}
		if(rec['Recurring_Discount_Type__c'] === 'Percentage' &&
			isNow(rec['Recurring_Discount_Start_Date__c'])
		) // 	3R. Recurring Line Percentage
		{
			var dVal = rec['Recurring_Deal_Currency_List_Price__c'] * (1-(rec['Recurring_Discount__c']/100));
			rec['SBQQ__ListPrice__c'] = dVal;
			rec['SBQQ__CustomerPrice__c'] = dVal;
			rec['Fair_Value__c'] = dVal;
			rec['Recurring_Net_Total__c'] = dVal;
			_debugRec(rec,'3R: '+dVal);			
		}
		//US Payments Bundle Push
		if(rec['Initial_Pricing_Filter__c'].includes('US Payments, 0, 0, 0'))
			vUSListPricenoTarget += rec['Initial_Deal_Currency_List_Price__c'];
		if(rec['Recurring_pricing_filter__c'].includes('US Payments, 0, 0, 0'))		
			vUSListPricenoTargetre += rec['Recurring_Deal_Currency_List_Price__c'];
		if(rec['Initial_Pricing_Filter__c'].includes('US Payments, 0, 0, 1'))	
			vUSLiveTarget += nvl(rec['Initial_Discount__c']);
		if(rec['Recurring_pricing_filter__c'].includes('US Payments, 0, 0, 1'))			
			vUSLiveTargetre += nvl(rec['Recurring_Discount__c']);
		if(rec['Initial_Pricing_Filter__c'].includes('US Payments'))
			vUSTotalRevIni+=rec['Initial_Deal_Currency_List_Price__c'];
		if(rec['Recurring_pricing_filter__c'].includes('US Payments'))		
			vUSTotalRevREC+=rec['Recurring_Deal_Currency_List_Price__c'];
		if(rec['Initial_Pricing_Filter__c'].includes('US Payments, 0, 0, 2'))
			vUSLivePercTotal+=nvl(rec['Sold_Value__c']);
		if(rec['Recurring_pricing_filter__c'].includes('US Payments, 0, 0, 2'))		
			vUSLivePercTotalRe+=nvl(rec['SBQQ__CustomerPrice__c']);


		//Global Payments Bundle Push
		if(rec['Initial_Pricing_Filter__c'].includes('Global Payments, 0, 0, 0'))
			vGLListPricenoTarget += rec['Initial_Deal_Currency_List_Price__c'];
		if(rec['Recurring_pricing_filter__c'].includes('Global Payments, 0, 0, 0'))
			vGLListPricenoTargetre += rec['Recurring_Deal_Currency_List_Price__c'];
		if(rec['Initial_Pricing_Filter__c'].includes('Global Payments, 0, 0, 1'))	
			vGLLiveTarget += nvl(rec['Initial_Discount__c']);
		if(rec['Recurring_pricing_filter__c'].includes('Global Payments, 0, 0, 1'))			
			vGLLiveTargetre += nvl(rec['Recurring_Discount__c']);
		if(rec['Initial_Pricing_Filter__c'].includes('Global Payments'))
			vGLTotalRevIni+=rec['Initial_Deal_Currency_List_Price__c'];
		if(rec['Recurring_pricing_filter__c'].includes('Global Payments'))
			vGLTotalRevREC+=rec['Recurring_Deal_Currency_List_Price__c'];
		if(rec['Initial_Pricing_Filter__c'].includes('Global Payments, 0, 0, 2'))
			vGLLivePercTotal+=nvl(rec['Sold_Value__c']);
		if(rec['Recurring_pricing_filter__c'].includes('Global Payments, 0, 0, 2'))
			vGLLivePercTotalRe+=nvl(rec['SBQQ__CustomerPrice__c']);
		if(rec['Recurring_pricing_filter__c'] != 'XYZ')
			RecListTotal += rec['Recurring_Deal_Currency_List_Price__c'];	

	});	


	lines.forEach(function(line) 
	{
		var rec = line.record;

		//US Payments Bundle Push
		if(rec['Initial_Pricing_Filter__c'].includes( 'US Payments') &&
			rec['Recurring_pricing_filter__c'].includes('US Payments'))
		{
			rec['Initial_Total_List_Price_No_Target__c'] = vUSListPricenoTarget;
			rec['Initial_Offering_Target_Total__c'] = vUSLiveTarget;
			rec['Recurring_Total_List_Price_No_Target__c'] = vUSListPricenoTargetre;
			rec['Recurring_Offering_Target_Total__c'] = vUSLiveTargetre;
			rec['Bundle_Initial_List_Total__c'] = vUSTotalRevIni;
			rec['Bundle_Recurring_List_Total__c'] = vUSTotalRevREC;
			rec['Initial_Total_Percentage_Post_Discount__c'] = vUSLivePercTotal;
			rec['Recurring_Total_Percentage_Post_Discount__c'] = vUSLivePercTotalRe;
			qre['Quote_Recurring_List_Total__c'] = RecListTotal;
			_debugRec(rec,'US Payments Bundle Push: vUSListPricenoTarget:'+vUSListPricenoTarget+' ,vUSLiveTarget:'+vUSLiveTarget);
		}
		//Global Payments Bundle Push
		if(rec['Initial_Pricing_Filter__c'].includes( 'Global Payments') &&
			rec['Recurring_pricing_filter__c'].includes('Global Payments'))
		{
			rec['Initial_Total_List_Price_No_Target__c'] = vGLListPricenoTarget;
			rec['Initial_Offering_Target_Total__c'] = vGLLiveTarget;
			rec['Recurring_Total_List_Price_No_Target__c'] = vGLListPricenoTargetre;
			rec['Recurring_Offering_Target_Total__c'] = vGLLiveTargetre;
			rec['Bundle_Initial_List_Total__c'] = vGLTotalRevIni;
			rec['Bundle_Recurring_List_Total__c'] = vGLTotalRevREC;
			rec['Initial_Total_Percentage_Post_Discount__c'] = vGLLivePercTotal;
			rec['Recurring_Total_Percentage_Post_Discount__c'] = vGLLivePercTotalRe;
			_debugRec(rec,'Global Payments Bundle Push: vGLListPricenoTarget:'+vGLListPricenoTarget+' ,vGLLiveTarget:'+vGLLiveTarget);
		}

	});
	//Quote Totals Bundle Push
	QuoteTotalsBundlePush(quote, lines);

	lines.forEach(function(line) 
	{

		var rec = line.record;
		var parent = line.parentItem;
		var parInitDiscount;
		var parRecurDiscount;
		if(parent != null)
		{
			parInitDiscount = parent.record['Initial_Discount__c'];
			parRecurDiscount = parent.record['Recurring_Discount__c'];
		}
		if(parInitDiscount == null)
			parInitDiscount = 0;
		if(parRecurDiscount == null)
			parRecurDiscount =0;

		RPA(qre,rec,mRPA);

		if(rec['Initial_Pricing_Filter_Target__c'] === 1) // 4I. Initial Bundle Target
		{
			var dVal = 			(rec['Initial_Deal_Currency_List_Price__c'] / rec['Initial_Total_List_Price_No_Target__c']) * 
			(parInitDiscount - (rec['Initial_Offering_Target_Total__c'] + rec['Initial_Total_Percentage_Post_Discount__c']));

			rec['Sold_Value__c'] = dVal;
			_debugRec(rec,'4I: '+dVal+'/'+rec['Initial_Deal_Currency_List_Price__c']);
		}

		if(rec['Recurring_Pricing_Filter_Target__c'] === 1) //4R. Recurring Bundle Target
		{
			var dVal = (rec['Recurring_Deal_Currency_List_Price__c'] / rec['Recurring_Total_List_Price_No_Target__c']) * 
			(parRecurDiscount - (rec['Recurring_Offering_Target_Total__c'] + rec['Recurring_Total_Percentage_Post_Discount__c']));
			rec['SBQQ__CustomerPrice__c'] = dVal;
			rec['SBQQ__ListPrice__c'] = dVal;
			rec['SBQQ__NetPrice__c'] = dVal;
			rec['Fair_Value__c'] = dVal;
			rec['Recurring_Net_Total__c'] = dVal;									

			_debugRec(rec,'4R: ('+rec['Recurring_Deal_Currency_List_Price__c']+' / '+rec['Recurring_Total_List_Price_No_Target__c']+') * ('+parRecurDiscount+' - ('+rec['Recurring_Offering_Target_Total__c']+' + '+rec['Recurring_Total_Percentage_Post_Discount__c']+'))='+dVal );			
		}
	//	if (rec['Ignore_Price_Rules__c'] != true)
		{
			if(rec['Initial_Pricing_Filter_Target__c'] === 2) //5I. Initial Bundle Percentage
			{
				var dVal = (rec['Initial_Deal_Currency_List_Price__c'] / rec['Initial_Total_List_Price_No_Target__c']) 
					* ( ((1-(parInitDiscount/100)) *rec['Bundle_Initial_List_Total__c']) - 
					(rec['Initial_Offering_Target_Total__c'] + rec['Initial_Total_Percentage_Post_Discount__c']));
				rec['Sold_Value__c'] = dVal;
				rec['ILF_Fair_Value__c'] = dVal;
				_debugRec(rec,'5I: '+dVal);	
			}

			if(rec['Recurring_Pricing_Filter_Target__c'] == 2) //5R. Recurring Bundle Percentage
			{
				var dVal = 
				(rec['Recurring_Deal_Currency_List_Price__c'] / rec['Recurring_Total_List_Price_No_Target__c']) * 
				( ((1-(parRecurDiscount/100)) *rec['Bundle_Recurring_List_Total__c']) 
					- (rec['Recurring_Offering_Target_Total__c'] + rec['Recurring_Total_Percentage_Post_Discount__c']));
				rec['SBQQ__NetPrice__c'] = dVal;
				rec['Fair_Value__c'] = dVal;
				rec['Recurring_Net_Total__c'] = dVal;
				_debugRec(rec,'5R: ('+rec['Recurring_Deal_Currency_List_Price__c']+' / '+rec['Recurring_Total_List_Price_No_Target__c']+') * ( ((1-('+parRecurDiscount+'/100)) *'+rec['Bundle_Recurring_List_Total__c']+') - ('+rec['Recurring_Offering_Target_Total__c']+' + '+rec['Recurring_Total_Percentage_Post_Discount__c']+'))='+dVal);

			}
		}
		//6I. Initial Quote Target
		if(rec['Initial_Pricing_Filter_Target__c'] ==0 && rec['Initial_Bundle_Pricing_Filter__c'] === 'ND' &&
			(rec['Initial_Pricing_Filter__c']).includes('0, 0, 0') && qre['Initial_Discount_Type__c'] === 'Target')
		{
			var dVal = 
(rec['Initial_Deal_Currency_List_Price__c'] / qre['Initial_Total_List_Price_No_Target__c']) * 
(qre['Initial_Discount__c'] - (qre['Initial_Total_Target_Post_Discount__c'] + qre['Initial_Total_Percentage_Post_Discount__c']) 
);
			rec['Sold_Value__c'] = dVal;
			rec['ILF_Fair_Value__c'] = dVal;
			_debugRec(rec,'6I: dVal=('+rec['Initial_Deal_Currency_List_Price__c']+' / '+qre['Initial_Total_List_Price_No_Target__c']+') * ('+qre['Initial_Discount__c']+' - ('+qre['Initial_Total_Target_Post_Discount__c']+' + '+qre['Initial_Total_Percentage_Post_Discount__c']+'))='+dVal+';');
		}
		//6R. Recurring Quote Target
		if(rec['Recurring_Pricing_Filter_Target__c'] == 0 && rec['Recurring_Bundle_Pricing_Filter__c'] === 'ND' &&
			qre['Recurring_Discount_Type__c'] === 'Target' && (rec['Recurring_pricing_filter__c']).includes('0, 0, 0')
			)
		{
			var dVal =

(rec['Recurring_Deal_Currency_List_Price__c'] / qre['Recurring_Total_List_Price_No_Target__c']) * 
(qre['Recurring_Discount__c'] - (rec['Recurring_Offering_Target_Total__c'] + rec['Recurring_Total_Percentage_Post_Discount__c']) 
);
			rec['SBQQ__ListPrice__c'] = dVal;
			rec['Fair_Value__c'] = dVal;
			rec['Recurring_Net_Total__c'] = dVal;
			_debugRec(rec,'6R: dVal=('+rec['Recurring_Deal_Currency_List_Price__c']+' / '+qre['Recurring_Total_List_Price_No_Target__c']+') * ('+qre['Recurring_Discount__c']+' - ('+qre['Recurring_Offering_Target_Total__c']+' + '+qre['Recurring_Total_Percentage_Post_Discount__c']+'))='+dVal+';');
		}

		//	7I. Initial Quote Percentage
		if(qre['Initial_Discount_Type__c'] === 'Percentage' && rec['Initial_Bundle_Pricing_Filter__c'] === 'ND' && 
			rec['Initial_Pricing_Filter_Target__c'] == 0 && (rec['Initial_Pricing_Filter__c']).includes('0, 0, 0')
			)
		{
			var dVal = 
rnd((rec['Initial_Deal_Currency_List_Price__c'] / qre['Initial_Total_List_Price_No_Target__c']) * 
( 
((1-qre['Initial_Discount__c']/100) 
*qre['Quote_Initial_List_Total__c']) 
- 
(qre['Initial_Total_Target_Post_Discount__c'] + qre['Initial_Total_Percentage_Post_Discount__c'])));
			rec['Sold_Value__c'] = dVal;
			rec['ILF_Fair_Value__c'] = dVal;
			_debugRec(rec,'7I: ('+rec['Initial_Deal_Currency_List_Price__c']+' / '+qre['Initial_Total_List_Price_No_Target__c']+') * (((1-'+qre['Initial_Discount__c']+'/100) *'+qre['Quote_Initial_List_Total__c']+') - ('+qre['Initial_Total_Target_Post_Discount__c']+' + '+qre['Initial_Total_Percentage_Post_Discount__c']+'))='+dVal);				
		}

		//7R. Recurring Quote Percentage
		if(qre['Recurring_Discount_Type__c'] === 'Percentage' && rec['Recurring_Bundle_Pricing_Filter__c'] === 'ND' &&
			rec['Recurring_Pricing_Filter_Target__c'] == 0 && (rec['Recurring_pricing_filter__c']).includes('0, 0, 0')
			)
		{
			var dVal = 
(rec['Recurring_Deal_Currency_List_Price__c'] / qre['Recurring_Total_List_Price_No_Target__c']) * 
( ((1-(nvl(qre['Recurring_Discount__c'])/100)) 
*qre['Quote_Recurring_List_Total__c']) 
- 
(rec['Recurring_Offering_Target_Total__c'] + qre['Recurring_Total_Percentage_Post_Discount__c']));
			rec['SBQQ__ListPrice__c'] = dVal;
			rec['Fair_Value__c'] = dVal;
			rec['Recurring_Net_Total__c'] = dVal;
			_debugRec(rec,'7R: '+dVal+'=('+rec['Recurring_Deal_Currency_List_Price__c']+' / '+qre['Recurring_Total_List_Price_No_Target__c']+') * ( ((1-('+nvl(qre['Recurring_Discount__c'])+'/100)) *'+qre['Quote_Recurring_List_Total__c']+') - ('+rec['Recurring_Offering_Target_Total__c']+' + '+qre['Recurring_Total_Percentage_Post_Discount__c']+'))');
		}

		//Percentage Cost Rule
		if(rec['X3rd_party_Cost_Type__c'] === 'Percent' && rec['Pearl_Item__c'] === '')
		{
			qre['ThirdParty_Costs_PO_Ccy__c'] = 	
			rec['Initial_3rd_party_Cost_List__c']*	rec['Initial_Deal_Currency_List_Price__c'];
			qre['ThirdParty_Recurring_Costs_PO_Ccy__c']=
			rec['Recurring_3rd_party_Cost_List__c']*	rec['Recurring_Deal_Currency_List_Price__c'];

			rec['ThirdParty_Costs_Sold_Ccy__c'] = 
			qre['Price_Conversion_Rate__c']*rec['ThirdParty_Costs_PO_Ccy__c'];
			rec['ThirdParty_Recurring_Costs_Sold_Ccy__c'] = 
			qre['Price_Conversion_Rate__c']*rec['ThirdParty_Recurring_Costs_PO_Ccy__c'];
		}

		//Fixed Cost Rule
		if(rec['X3rd_party_Cost_Type__c'] === 'Fixed' || rec['Pearl_Item__c'] === '')
		{
			rec['ThirdParty_Recurring_Costs_PO_Ccy__c'] = rec['Recurring_3rd_Party_Cost_List__c'];
			rec['ThirdParty_Costs_PO_Ccy__c'] = rec['Initial_3rd_Party_Cost_List__c'];
			rec['ThirdParty_Costs_Sold_Ccy__c'] =  qre['Price_Conversion_Rate__c'] * qre['ThirdParty_Costs_PO_Ccy__c'];
			rec['ThirdParty_Recurring_Costs_Sold_Ccy__c'] = qre['Price_Conversion_Rate__c']*qre['ThirdParty_Recurring_Costs_PO_Ccy__c'];
		}
		rec['X3P_Termination_Fee__c'] = 0;
		if(qre['Ignore_Contract_Termination__c'] !== true)
		{
			if(qre['Deal_Type__c'] =='Termination/Read-only' && rec['External_SKU_DH__c'] !== '463-28672' && rec['SBQQ_TerminatedDate__c'] != null)
			{
				var term =getTerm2(rec["SBQQ__StartDate__c"],qre["SBQQ__StartDate__c"],rec['SBQQ_TerminatedDate__c']);
				_debugRec(rec, 'termination term '+term+' '+rec["SBQQ__StartDate__c"]+' '+rec["SBQQ__EndDate__c"]);
				rec['X3P_Termination_Fee__c'] = nvl(rec["ThirdParty_Recurring_Costs_Sold_Ccy__c"]) * term;

			}			
		}
		_debugRec(rec,'1.SBQQ__ListPrice__c '+rec['SBQQ__ListPrice__c']+' SBQQ__CustomerPrice__c '+rec['SBQQ__CustomerPrice__c']+' SBQQ__NetPrice__c '+rec['SBQQ__CustomerPrice__c']);

		var term = qre['SBQQ__SubscriptionTerm__c']/12;
		//SEV rev rec calc : TSV
		rec['TSV_Net_Price__c'] = rec['Recurring_Net_Total__c'] *term;
		rec['TSV_List_Price__c'] = rec['Recurring_Deal_Currency_List_Price__c'] *term;
		rec['TSV_RPA_Price__c'] = rec['Recurring_RPA_List_Price__c'] *term;

			rec['Recurring_SSP_Lower__c']  =0;
			rec['Recurring_SSP_Upper__c']  = 0;
			rec['TSV_SSP_Lower__c']  =0;
			rec['TSV_SSP_Upper__c']  = 0;	
			rec['Initial_SSP_Lower__c']  = 0;
			rec['Initial_SSP_Upper__c']  = 0;

		if(rec['ISSPT__c'] != null)
		{
			if(rec['ISSPT__c'] ==='% of List')
			{
				rec['Initial_SSP_Lower__c']  = rnd(rec['ISSPL__c']*rec['Initial_Deal_Currency_List_Price__c']);
				rec['Initial_SSP_Upper__c']  = rnd(rec['ISSPU__c']*rec['Initial_Deal_Currency_List_Price__c']);
//				_debugRec(rec,'ISSPL('+rec['ISSPT__c']+')='+rec['ISSPL__c']+'*'+rec['Initial_Deal_Currency_List_Price__c']+'='+rec['Initial_SSP_Lower__c']);
//				_debugRec(rec,'ISSPU('+rec['ISSPT__c']+')='+rec['ISSPU__c']+'*'+rec['Initial_Deal_Currency_List_Price__c']+'='+rec['Initial_SSP_Upper__c']);

			}
			else if(rec['ISSPT__c'] ==='% of Net')
			{
				rec['Initial_SSP_Lower__c']  = rnd(rec['ISSPL__c']*rec['Sold_Value__c']);
				rec['Initial_SSP_Upper__c']  = rnd(rec['ISSPU__c']*rec['Sold_Value__c']);
//				_debugRec(rec,'ISSPL('+rec['ISSPT__c']+')='+rec['ISSPL__c']+'*'+rec['Sold_Value__c']+'='+rec['Initial_SSP_Lower__c']);
//				_debugRec(rec,'ISSPU('+rec['ISSPT__c']+')='+rec['ISSPU__c']+'*'+rec['Sold_Value__c']+'='+rec['Initial_SSP_Upper__c']);

			}
			else
			{

//				_debugRec(rec,'ISSPL=0');
//				_debugRec(rec,'ISSPU=0');				
			}
		}
		if(rec['RSSPT__c'] != null)
		{
			if(rec['RSSPT__c'] ==='% of List')
			{
				rec['Recurring_SSP_Lower__c']  = rnd(rec['RSSPL__c']*rec['Recurring_Deal_Currency_List_Price__c']);
				rec['Recurring_SSP_Upper__c']  = rnd(rec['RSSPU__c']*rec['Recurring_Deal_Currency_List_Price__c']);
				rec['TSV_SSP_Lower__c']  = rnd(rec['RSSPL__c']*rec['TSV_List_Price__c']);
				rec['TSV_SSP_Upper__c']  = rnd(rec['RSSPU__c']*rec['TSV_List_Price__c']);	
//				_debugRec(rec,'RSSPL('+rec['RSSPT__c']+')='+rec['RSSPL__c']+'*'+rec['Recurring_Deal_Currency_List_Price__c']+'='+rec['Recurring_SSP_Lower__c']);
//				_debugRec(rec,'RSSPU('+rec['RSSPT__c']+')='+rec['RSSPU__c']+'*'+rec['Recurring_Deal_Currency_List_Price__c']+'='+rec['Recurring_SSP_Upper__c']);
			}
			else if(rec['RSSPT__c'] ==='% of Net')
			{
				rec['Recurring_SSP_Lower__c']  = rnd(rec['RSSPL__c']*rec['Recurring_Net_Total__c']);
				rec['Recurring_SSP_Upper__c']  = rnd(rec['RSSPU__c']*rec['Recurring_Net_Total__c']);
				rec['TSV_SSP_Lower__c']  = rnd(rec['RSSPL__c']*rec['TSV_Net_Price__c']);
				rec['TSV_SSP_Upper__c']  = rnd(rec['RSSPU__c']*rec['TSV_Net_Price__c']);
//				_debugRec(rec,'RSSPL('+rec['RSSPT__c']+')='+rec['RSSPL__c']+'*'+rec['Recurring_Net_Total__c']+'='+rec['Recurring_SSP_Lower__c']);
//				_debugRec(rec,'RSSPU('+rec['RSSPT__c']+')='+rec['RSSPU__c']+'*'+rec['Recurring_Net_Total__c']+'='+rec['Recurring_SSP_Upper__c']);
			}
			else if(rec['RSSPT__c'] ==='% of Initial List')
			{
				rec['Recurring_SSP_Lower__c']  = rnd(rec['RSSPL__c']*rec['Initial_Deal_Currency_List_Price__c']);
				rec['Recurring_SSP_Upper__c']  = rnd(rec['RSSPU__c']*rec['Initial_Deal_Currency_List_Price__c']);
				rec['TSV_SSP_Lower__c']  = rnd(rec['RSSPL__c']*rec['Initial_Deal_Currency_List_Price__c']);
				rec['TSV_SSP_Upper__c']  = rnd(rec['RSSPU__c']*rec['Initial_Deal_Currency_List_Price__c']);	
//				_debugRec(rec,'RSSPL('+rec['RSSPT__c']+')='+rec['RSSPL__c']+'*'+rec['Initial_Deal_Currency_List_Price__c']+'='+rec['Recurring_SSP_Lower__c']);								
//				_debugRec(rec,'RSSPU('+rec['RSSPT__c']+')='+rec['RSSPU__c']+'*'+rec['Initial_Deal_Currency_List_Price__c']+'='+rec['Recurring_SSP_Upper__c']);								

			}
			else if(rec['RSSPT__c'] ==='% of Initial Net')
			{
				rec['Recurring_SSP_Lower__c']  = rnd(rec['RSSPL__c']*rec['Sold_Value__c']);
				rec['Recurring_SSP_Upper__c']  = rnd(rec['RSSPU__c']*rec['Sold_Value__c']);
				rec['TSV_SSP_Lower__c']  = rnd(rec['RSSPL__c']*rec['Sold_Value__c']);
				rec['TSV_SSP_Upper__c']  = rnd(rec['RSSPU__c']*rec['Sold_Value__c']);
//				_debugRec(rec,'RSSPL('+rec['RSSPT__c']+')='+rec['RSSPL__c']+'*'+rec['Sold_Value__c']+'='+rec['Recurring_SSP_Lower__c']);													
//				_debugRec(rec,'RSSPU('+rec['RSSPT__c']+')='+rec['RSSPU__c']+'*'+rec['Sold_Value__c']+'='+rec['Recurring_SSP_Upper__c']);													

			}
			else
			{
//				_debugRec(rec,'RSSP('+rec['RSSPT__c']+')=0');																
			}
//			_debugRec(rec,'TSV_SSP:['+rec['TSV_SSP_Lower__c']+':'+rec['TSV_SSP_Upper__c']+']');
//			_debugRec(rec,'Recurring_SSP:['+rec['Recurring_SSP_Lower__c']+':'+rec['Recurring_SSP_Upper__c']+']');		
		}
	});
	AssetUplift(quote,lines);

	QuotePush(quote,lines);

	return Promise.resolve();
}
function RPA(qre,rec,mRPA)
{
	rec['Initial_RPA_List_Price__c'] = rec['Initial_Deal_Currency_List_Price__c'];
	rec['Recurring_RPA_List_Price__c'] = rec['Recurring_Deal_Currency_List_Price__c'];

	//RPA Discounts
	var inRatePerc = 30;
	var recRatePerc = 30;

	if(rec['Sales_Region__c'] != '' && rec['Sales_Product_Set__c'] != '')
	{
		var sKey = rec['Sales_Product_Set__c']+'__'+rec['Sales_Region__c'] ;
		if(mRPA != null)
		{
			var rpa = mRPA.get(sKey);
//			_debug('RPA getting for sKey: '+sKey+' rpa:'+rpa);
			if(rpa != null)
			{
				inRatePerc = nvl(rpa.Initial_Rate__c);
				recRatePerc = nvl(rpa.Recurring_Rate__c);
			}
		}
	}
	if(qre['Deal_Type__c'] == 'New Name Customer')
	{
		if(nvl(qre['Quote_Initial_Net_Total__c']) > 500000 || nvl(qre['Quote_Recurring_Net_Total__c']) > 200000)
		{
			recRatePerc +=10
			inRatePerc +=10;
		}
	}else if(qre['Deal_Type__c'] == 'Volume Increase' && qre['Licence_Compliance_Period__c'] != null && qre['Licence_Compliance_Period__c'].includes('LC Only'))
	{
		inRatePerc =0;
		recRatePerc = 0;
	}
	rec['Initial_RPA_Discount__c'] = inRatePerc;
	rec['Recurring_RPA_Discount__c'] = recRatePerc;
	var inDiscRate  = (1-(inRatePerc/100));
	var reDiscRate = (1-(recRatePerc/100));
	rec['Initial_RPA_List_Price__c'] = rec['Initial_Deal_Currency_List_Price__c']*inDiscRate;
	rec['Recurring_RPA_List_Price__c'] = rec['Recurring_Deal_Currency_List_Price__c']*reDiscRate;
	_debugRec(rec,'RPA: initPrice: '+rec['Initial_Deal_Currency_List_Price__c']+' inRate: '+rec['Initial_RPA_Discount__c']+' inDiscRate: '+inDiscRate+' inRPA: '+rec['Initial_RPA_List_Price__c']);
	_debugRec(rec,'RPA: recPrice: '+rec['Recurring_Deal_Currency_List_Price__c']+' reRate: '+rec['Recurring_RPA_Discount__c']+' reDiscRate: '+reDiscRate+' reRPA: '+rec['Recurring_RPA_List_Price__c']);

	rec['Initial_RPA_Total__c'] = rec['Initial_RPA_List_Price__c']* rec['SBQQ__Quantity__c'];
	rec['Recurring_RPA_Total__c'] = rec['Recurring_RPA_List_Price__c']* rec['SBQQ__Quantity__c'];		

}
function AssetUplift(quote, lines)
{
	var AssetUpliftRPABase=0;
	var qre = quote.record; 
	var aAsset = [];
	lines.forEach(function(line) 
	{
		var rec = line.record;
		if(rec['Billing_Method__c'] === 'Asset')
		{
			if(nvl(rec['Initial_RPA_List_Price__c']) != 0)
			{
				if(rec['Asset_Uplift_Source__c'] === null)
				{
					throw ('Please specify Asset Uplift Source for '+rec['SBQQ__ProductName__c']);
				}
				if(rec['Asset_Uplift_Source__c'] !== 'Manual' && rec['Amount_For_Uplift__c'] === 0)
				{
					throw ('Please specify '+rec['Asset_Uplift_Source__c']+' in Sales Survey');
				}
			}
			AssetUpliftRPABase += nvl(rec['Initial_RPA_List_Price__c']);
			_debugRec(rec,'AssetUpliftRPABase+='+nvl(rec['Initial_RPA_List_Price__c']));
			aAsset.push(line);
		}
		else
		{
			rec['RLF_Uplift_Multiplier_Percent__c'] = 0;
			rec['Uplift_Multiplier__c'] = 0;
		}
	});
	if(AssetUpliftRPABase != 0 && qre['Deal_Type__c'] !='Termination/Read-only')
	{
/*		if(nvl(qre['Asset_Size__c']) == 0)
		{
			throw ('Please specify the Asset Size in Sales Survey.');		
		}*/
		if(nvl(qre['Uplift_Multiplier__c']) == 0)
		{
			throw ('Please specify Initial Uplift Multiplier in Sales Survey.');
		}
		if(nvl(qre['RLF_Uplift_Multiplier_Percent__c'])==0)
		{
			throw ('Please specify Recurring Uplift Multiplier Percent in Sales Survey.');
		}

		aAsset.forEach(function(line) 
		{	
			var rec = line.record;	
			rec['Uplift_Multiplier__c'] = nvl(qre['Uplift_Multiplier__c'])*nvl(rec['Initial_RPA_List_Price__c'])/AssetUpliftRPABase;
			_debugRec(rec,'Uplift_Multiplier='+rec['Uplift_Multiplier__c']+'='+nvl(qre['Uplift_Multiplier__c'])+'*'+nvl(rec['Initial_RPA_List_Price__c'])+'/'+AssetUpliftRPABase);
			rec['RLF_Uplift_Multiplier_Percent__c'] = qre['RLF_Uplift_Multiplier_Percent__c'];
		});
	}
}

//Quote Totals Bundle Push
function QuoteTotalsBundlePush(quote, lines)
{
//	_debug('Quote Totals Bundle Push start');
	var InTargetTotalIncBD=0,InPercTotalIncBD=0,InListPriceNoTarget=0
	,InListTotal=0,RecListPricenoTarget=0,RecPercTotalIncBD=0
	,RecTargetTotalIncBD=0,RecListTotal=0,RecNetTotal=0,InNetTotal=0
	,InTargetTotalBDO=0,RecTargetTotalBDO=0,RecPercTotalBDO=0,InPercTotalBDO=0;

	var CLSListPricenoTarget =0,CLSListPricenoTargetre =0,CLSLivePercentageTotal =0,
	CLSLivePercentageTotalre =0,CLSLiveTargetTotal =0,CLSLiveTargetTotalre =0,
	CLSTotalRevIni =0,CLSTotalRevREC =0;
	var qre = quote.record; 

	var aCLS = []; 

	lines.forEach(function(line) 
	{
		var rec = line.record;
		

		if(rec['Initial_Pricing_Filter__c'].includes('1'))
			InTargetTotalIncBD +=nvl(rec['Initial_Discount__c']);
		if(rec['Initial_Pricing_Filter__c'].includes('2'))
			InPercTotalIncBD +=nvl(rec['Sold_Value__c']);
		if(rec['Initial_Quote_Filter__c'].includes('0, 0, 0-ND'))
			InListPriceNoTarget += rec['Initial_Deal_Currency_List_Price__c'];

		if(rec['Recurring_Quote_Filter__c'].includes('0, 0, 0-ND'))
			RecListPricenoTarget += rec['Recurring_Deal_Currency_List_Price__c'];
		if(rec['Recurring_pricing_filter__c'].includes('2'))
			RecPercTotalIncBD += nvl(rec['Recurring_Net_Total__c']);
		if(rec['Recurring_pricing_filter__c'].includes('1'))
			RecTargetTotalIncBD += nvl(rec['Recurring_Discount__c']);
		if(rec['Recurring_pricing_filter__c'] != 'XYZ')
		{
			RecListTotal += rec['Recurring_Deal_Currency_List_Price__c'];	
			RecNetTotal += nvl(rec['Recurring_Net_Total__c']);						
		}
		if(rec['Initial_Pricing_Filter__c'] != 'XYZ')
		{
			InListTotal += rec['Initial_Deal_Currency_List_Price__c'];
			InNetTotal += nvl(rec['Sold_Value__c']);
		}
		if(rec['Initial_Quote_Filter__c'].includes('1-BD'))
			InTargetTotalBDO += nvl(rec['Initial_Discount__c']);
		if(rec['Recurring_Quote_Filter__c'].includes('1-BD'))
			RecTargetTotalBDO += nvl(rec['Recurring_Discount__c']);
		if(rec['Initial_Quote_Filter__c'].includes('2-BD'))
			InPercTotalBDO += nvl(rec['Initial_Discount__c']);
		if(rec['Recurring_Quote_Filter__c'].includes('2-BD'))
			RecPercTotalBDO += nvl(rec['Recurring_Discount__c']);


		if(rec['Initial_Pricing_Filter__c'].includes('CLS, 0, 0, 0'))
			CLSListPricenoTarget += rec['Initial_Deal_Currency_List_Price__c'];

		if(rec['Recurring_pricing_filter__c'].includes('CLS, 0, 0, 0'))
			CLSListPricenoTargetre += rec['Recurring_Deal_Currency_List_Price__c'];


		if(rec['Initial_Pricing_Filter__c'].includes('CLS, 0, 0, 2'))
			CLSLivePercentageTotal += nvl(rec['Sold_Value__c']);

		if(rec['Recurring_pricing_filter__c'].includes('CLS, 0, 0, 2'))
			CLSLivePercentageTotalre += nvl(rec['Recurring_Net_Total__c']);

		if(rec['Initial_Pricing_Filter__c'].includes('CLS, 0, 0, 1'))
			CLSLiveTargetTotal += nvl(rec['Initial_Discount__c']);

		if(rec['Recurring_pricing_filter__c'].includes('CLS, 0, 0, 1'))
			CLSLiveTargetTotalre += nvl(rec['Recurring_Discount__c']);


		if(rec['Initial_Pricing_Filter__c'].includes('CLS'))
			CLSTotalRevIni += rec['Initial_Deal_Currency_List_Price__c'];

		if(rec['Recurring_pricing_filter__c'].includes('CLS'))
			CLSTotalRevREC += rec['Recurring_Deal_Currency_List_Price__c'];
		

		if(rec['Initial_Pricing_Filter__c'].includes('CLS') && 
			rec['Recurring_pricing_filter__c'].includes('CLS')
			)
			aCLS.push(line);
	});

	qre['Total_Quote_Initial_Target__c']=InTargetTotalIncBD;
	qre['Total_Quote_Initial_Percentage__c']=InPercTotalIncBD;
	qre['Initial_Total_List_Price_No_Target__c']=InListPriceNoTarget;	
	qre['Quote_Initial_List_Total__c']=InListTotal;
	qre['Recurring_Total_List_Price_No_Target__c']=RecListPricenoTarget;
	qre['Total_Quote_Recurring_Percentage__c']=RecPercTotalIncBD;
	qre['Total_Quote_Recurring_Target__c']=RecTargetTotalIncBD;
	qre['Quote_Recurring_List_Total__c']=RecListTotal;
	qre['Quote_Recurring_Net_Total__c']=RecNetTotal;
	qre['Quote_Initial_Net_Total__c']=InNetTotal;
	qre['Bundle_Initial_Target_Total__c']=InTargetTotalBDO;
	qre['Bundle_Recurring_Target_Total__c']=RecTargetTotalBDO;
	qre['Bundle_Recurring_Percentage_Total__c']=RecPercTotalBDO;
	qre['Bundle_Initial_Percentage_Total__c']=InPercTotalBDO;
//CLS Bundle Push
	aCLS.forEach(function(line) 
	{
		var rec = line.record;	
		rec['Initial_Total_List_Price_No_Target__c'] = CLSListPricenoTarget;
		rec['Recurring_Total_List_Price_No_Target__c'] = CLSListPricenoTargetre;
		rec['Initial_Total_Percentage_Post_Discount__c'] = CLSLivePercentageTotal;
		rec['Recurring_Total_Percentage_Post_Discount__c'] = CLSLivePercentageTotalre;
		rec['Initial_Offering_Target_Total__c'] = CLSLiveTargetTotal;
		rec['Recurring_Offering_Target_Total__c'] = CLSLiveTargetTotalre;
		rec['Bundle_Initial_List_Total__c'] = CLSTotalRevIni;
		rec['Bundle_Recurring_List_Total__c'] = CLSTotalRevREC;
	});

}

function aggregateGroup(ret,rec,sKey,sspt,net,listPrice,rpa,sspl,sspu)
{

	var el; // geting aggregated values by 'Rev Type'+ 
	if(sKey in ret) //if there is no entry for this revenue type - lets create and add.
	{
		el = ret[sKey];
	}
	else
	{
		el = 
		{
			sspl :0,
			sspu :0,
			net: 0,
			list:0,
			rpa:0,
			netResidual:0,
			rpaResidual:0,
			listResidual:0,
			netNonResid:0,
			rpaNonResid:0,
			listNonResid:0,			
			bResidual: false,
			arrResidual :[],
			nonResidual :[]
			,allRecs:[]
		};
		ret[sKey]=el;
	}
	if(sspt == 'Residual')
	{
		el.arrResidual.push(rec);
		el.bResidual = true;
		el.netResidual +=net;
		el.rpaResidual +=rpa;
		el.listResidual+=listPrice;

	}else
	{
		el.nonResidual.push(rec);
		el.netNonResid +=net;
		el.rpaNonResid +=rpa;
		el.listNonResid+=listPrice;		
	}
	el.allRecs.push(rec);

	el.net  += net;
	el.list +=listPrice;
	el.rpa  += rpa;
	el.sspl += sspl;
	el.sspu += sspu;

	return el;
}
function sumQuote(aGrp,ret,type)
{
	if(ret == null)
	ret = 
	{
		net:0,
		rpa:0,
		alpha:0,
		rpaResidual :0,
		rpaNonResid :0,
		netResidual :0,
		bResidual: false
	}
	for(var sKey in aGrp)
	{
		var grp = aGrp[sKey];
		grp.alpha = alpha(grp.sspl, grp.net,grp.sspu);
		_debug(type+'.alpha('+sKey+')=alpha('+grp.sspl+','+grp.net+','+grp.sspu+')='+grp.alpha);
		ret.net += grp.net;
		ret.rpa += grp.rpa;
		ret.alpha += grp.alpha;
		ret.rpaResidual += grp.rpaResidual;
		ret.rpaNonResid += grp.rpaNonResid;
		ret.netResidual += grp.netResidual;
		ret.bResidual = ret.bResidual || grp.bResidual;
	}
	return ret;
}
function alpha(sspl,net,sspu)
{
	return (net>sspu?sspu:(net<sspl?sspl:net));
}

function getSSPKeyR(rec)
{
//	return 'R_'+rec['Recurring_Revenue_Type__c']+'_'+rec['RSSPT__c'];
	return rec['Recurring_Revenue_Type__c'];
}
function getSSPKeyI(rec)
{
//	return 'I_'+rec['Initial_Revenue_Type__c']+'_'+rec['ISSPT__c'];
	return rec['Initial_Revenue_Type__c'];
}

function QuotePush(quote, lines)
{
	var qre = quote.record; 
	var grpInit = {};
	var grpTsv = {};

	lines.forEach(function(line) 
	{
		var rec = line.record;
		aggregateGroup(
			grpInit
			,rec
			,getSSPKeyI(rec)
			,rec['ISSPT__c']
			,rec['Sold_Value__c'] // roll-up by revType+ISSPT 
			,rec['Initial_Deal_Currency_List_Price__c']
			,rec['Initial_RPA_List_Price__c']
			,rec['Initial_SSP_Lower__c']
			,rec['Initial_SSP_Upper__c']
		);

		aggregateGroup(
			grpTsv
			,rec
			,getSSPKeyR(rec)
			,rec['RSSPT__c']
			,rec['TSV_Net_Price__c']
			,rec['TSV_List_Price__c']
			,rec['TSV_RPA_Price__c']
			,rec['TSV_SSP_Lower__c']
			,rec['TSV_SSP_Upper__c']
		);
	});


	var quot = sumQuote(grpInit,null,'Init');
	sumQuote(grpTsv,quot,'TSV');	
	calcFairValue(grpInit,quot,'ILF_Fair_Value__c'/*'Initial_FV__c'*/,'Sold_Value__c','Initial_RPA_List_Price__c');
	calcFairValue(grpInit,quot,'Annual_Fair_Value__c'/*'TSV_FV__c'*/,'TSV_Net_Price__c','TSV_RPA_Price__c');	


	var term = qre['SBQQ__SubscriptionTerm__c']/12;	
	qre['Recurring_FV__c'] =0;
	qre['TSV_FV__c'] =0;
	qre['Initial_FV__c'] =0;
	qre['Recurring_RPA_Total__c']=0;
	qre['Initial_RPA_Total__c']=0;
	lines.forEach(function(line) 
	{
		var rec = line.record;
		rec['Recurring_FV__c'] = rec['Annual_Fair_Value__c'/*'TSV_FV__c'*/]/term;
		qre['Recurring_FV__c'] += rec['Recurring_FV__c'];		
		qre['TSV_FV__c'] += rec['Annual_Fair_Value__c'/*'TSV_FV__c'*/];
		qre['Initial_FV__c'] += rec['ILF_Fair_Value__c'/*'Initial_FV__c'*/];
		qre['Recurring_RPA_Total__c']+=nvl(rec['Recurring_RPA_Total__c']);
		qre['Initial_RPA_Total__c'] +=nvl(rec['Initial_RPA_Total__c']);
		_debugRec(rec,'FV '+rec['SBQQ__ProductName__c']+' init:'+rec['ILF_Fair_Value__c'/*'Initial_FV__c'*/]+' TSV:'+rec['Annual_Fair_Value__c'/*'TSV_FV__c'*/]+' recur:'+rec['Recurring_FV__c']);
	});


	QuotePush0(quote, lines);
}



function QuotePush0(quote, lines)
{
		var qre = quote.record; 

var Pre_Carve_Out_List_Price_PS_FP__c =0
,Recurring_RPA_Total__c =0
,Pre_Carve_Out_Sell_Price_RA1_Deal__c=0
,Pre_Carve_Out_Sell_Price_Recurring_RA1__c=0
,Pre_Carve_Out_Sell_Price_RA2_Deal__c=0
,Pre_CO_Sell_Price_RLF_from_RA2_annual__c=0
,Pre_Carve_Out_Sell_Price_Subs_Deal__c=0
,Pre_Carve_Out_Sell_Price_Sub__c=0
,Pre_COut_Sell_Price_RA1_Subs_Deal__c=0
,Pre_CO_Sell_Price_Recurring_RA1_Subs__c=0
,Pre_Carve_Out_List_Price_Packaged_CloudO__c=0
,Pre_Carve_Out_List_Price_Packaged_CloudA__c=0
,Pre_Carve_Out_List_Price_Packaged_CloudT__c=0
,Pre_Carve_Out_Sell_Price_RLF_from_ILF__c=0
,Pre_Carve_Out_List_Price_Cloud_ServicesO__c=0
,Pre_Carve_Out_List_Price_Cloud_ServicesA__c=0
,Pre_Carve_Out_List_Price_PSS_FP__c=0
,Pre_Carve_Out_List_Price_PSS_T_M__c=0
,Pre_CO_Initial_List_Price_PSS_Extended__c=0
,Pre_Carve_Out_List_Price_PSS_ExtendedA__c=0
,Pre_Carve_Out_Sell_Price_PSS_FP_Deal__c=0
,Pre_Carve_Out_Sell_Price_PSS_T_M_Deal__c=0
,Pre_CO_Initial_Net_Price_PSS_Extended__c=0
,Pre_CO_Sell_Price_PSS_Ext_Support_Deal__c=0
,Pre_Carve_Out_List_Price_PS_T_M__c=0
//,Pre_Carve_Out_List_Price_PS_FP__c=0
,Pre_Carve_Out_List_Price_FED_T_M__c=0
,Pre_C_O_List_Price_Recurring_FED_T_M__c=0
,Pre_Carve_Out_List_Price_FED_FP__c=0
,Pre_C_O_List_Price_Recurring_FED_FP__c=0
,Pre_Carve_Out_Sell_Price_PS_T_M__c=0
,Pre_Carve_Out_Sell_Price_PS_FP__c=0
,Pre_Carve_OutvSell_Price_FED_T_M_Deal__c=0
,Pre_C_O_Sell_Price_Recurring_FED_T_M__c=0
,Pre_Carve_Out_Sell_Price_FED_FP_Deal__c=0
,Pre_C_O_Recurring_Sell_Price_FED_FP__c=0
,Post_Carve_Out_Revenue_ILF__c=0
,Post_Carve_Out_Revenue_RLF_from_ILF__c=0
,Post_Carve_Rev_RA1_Deal__c=0
,Post_Carve_Rev_RLF_RA1_Deal__c=0
,Post_Carve_Rev_RA2_Deal__c=0
,Post_Carve_Rev_RLF_from_RA2_annual_Deal__c=0
,Post_Carve_Rev_Subscription_one_off_Deal__c=0
,Post_Carve_Rev_Subscription_Recurring__c=0
,Post_Carve_Rev_RA1_Subscription_Deal__c=0
,Post_Carve_Rev_RA1_Recur_Subscription__c=0
,Post_Carve_Rev_PSS_FP_Deal__c=0
,Post_Carve_Rev_PSS_T_M_Deal__c=0
,Post_Carve_Rev_PSS_Ext_Sup_Initial_Fee__c=0
,Post_Carve_Rev_PSS_Ext_Sup_annual_Deal__c=0
,Post_Carve_Out_Revenue_PS_T_M__c=0
,Post_Carve_Out_Revenue_PS_FP__c=0
,Post_Carve_Rev_FED_T_M_Deal__c=0
,Post_Carve_Rev_FED_T_M_Recurring__c=0
,Post_Carve_Rev_FED_FP_Deal__c=0
,Post_Carve_Rev_FED_FP_Recurring__c=0
,Post_Carve_Rev_Packaged_Cloud_one_off__c=0
,Post_Carve_Rev_Package_Cloud_annual_Deal__c=0
,Post_Carve_Rev_Packaged_Cloud_Transacti__c=0
,Post_Carve_Rev_Cloud_Services_one_off__c=0
,Pre_Carve_Out_List_Price_ILF_Deal__c=0
,Pre_Carve_Out_List_Price_RLF_from_ILF__c=0
,Pre_Carve_Out_List_Price_RA1__c=0
,Pre_Carve_Out_List_Price_Recurring_RA1__c=0
,Pre_Carve_Out_List_Price_RA2__c=0
,Pre_Carve_Out_List_Price_RLF_from_RA2A__c=0
,Pre_Carve_Out_List_Price_Subscription_On__c=0
,Pre_Carve_Out_List_Price_Subscription_An__c=0
,Pre_Carve_Out_List_Price_RA1_Sub__c=0
,Pre_CO_List_Price_Recurring_Fee_RA1_Sub__c=0
,Pre_Carve_Out_Sell_Price_ILF__c=0
,Post_Carve_Rev_Cloud_Service_annual_Deal__c=0
,Pre_CO_Sell_Price_Packaged_Cloud_One_off__c=0
,Pre_CO_Sell_Price_Packaged_Cloud_Annual__c=0
,Pre_CO_Sell_Price_Packaged_Cloud_Transac__c=0
,Pre_CO_SP_Cloud_Services_one_off_Deal__c=0
,Pre_Carve_OutSell_Price_Cloud_Services__c=0
,Initial_RPA_Total__c=0
,Pre_Carve_Out_List_Price_PS_T_M__c=0
,Termination_Fee__c=0;

	lines.forEach(function(line) 
	{
		var rec = line.record;
		if(rec['Initial_Revenue_Type__c']==='SVFP') Pre_Carve_Out_List_Price_PS_FP__c+=nvl(rec['Comm_Man_Price__c']);
		if(true) Recurring_RPA_Total__c+=nvl(rec['Recurring_RPA_Total__c']);
		if(rec['Initial_Revenue_Type__c']==='DVC1') Pre_Carve_Out_Sell_Price_RA1_Deal__c+=nvl(rec['Sold_Value__c']);
		if(rec['Recurring_Revenue_Type__c']==='DVC1') Pre_Carve_Out_Sell_Price_Recurring_RA1+=nvl(rec['Annual_Net_Price__c']);
		if(rec['Initial_Revenue_Type__c']==='DVF2') Pre_Carve_Out_Sell_Price_RA2_Deal+=nvl(rec['Sold_Value__c']);
		if(rec['Recurring_Revenue_Type__c']==='DVT2') Pre_CO_Sell_Price_RLF_from_RA2_annual__c+=nvl(rec['Annual_Net_Price__c']);
		if(rec['Initial_Revenue_Type__c']==='Sub') Pre_Carve_Out_Sell_Price_Subs_Deal__c+=nvl(rec['Sold_Value__c']);
		if(rec['Recurring_Revenue_Type__c']==='Sub') Pre_Carve_Out_Sell_Price_Sub__c+=nvl(rec['Annual_Net_Price__c']);
		if(rec['Initial_Revenue_Type__c']==='DVCC') Pre_COut_Sell_Price_RA1_Subs_Deal__c+=nvl(rec['Sold_Value__c']);
		if(rec['Recurring_Revenue_Type__c']==='DVCS') Pre_CO_Sell_Price_Recurring_RA1_Subs+=nvl(rec['Annual_Net_Price__c']);
		if(rec['Initial_Revenue_Type__c']==='ILCS') Pre_Carve_Out_List_Price_Packaged_CloudO__c+=nvl(rec['Comm_Man_Price__c']);
		if(rec['Recurring_Revenue_Type__c']==='ILCS') Pre_Carve_Out_List_Price_Packaged_CloudA__c+=nvl(rec['Annual_list_Price__c']);
		if(rec['Recurring_Revenue_Type__c']==='Tran') Pre_Carve_Out_List_Price_Packaged_CloudT+=nvl(rec['Annual_list_Price__c']);
		if(rec['Recurring_Revenue_Type__c']==='RLF') Pre_Carve_Out_Sell_Price_RLF_from_ILF__c+=nvl(rec['Annual_Net_Price__c']);
		if(rec['Initial_Revenue_Type__c']==='ILCF') Pre_Carve_Out_List_Price_Cloud_ServicesO__c+=nvl(rec['Comm_Man_Price__c']);
		if(rec['Recurring_Revenue_Type__c']==='ILCS') Pre_Carve_Out_List_Price_Cloud_ServicesA__c+=nvl(rec['Annual_list_Price__c']);
		if(rec['Initial_Revenue_Type__c']==='SPTM') Pre_Carve_Out_List_Price_PSS_FP__c+=nvl(rec['Comm_Man_Price__c']);
		if(rec['Initial_Revenue_Type__c']==='SPFP') Pre_Carve_Out_List_Price_PSS_T_M__c+=nvl(rec['Comm_Man_Price__c']);
		if(rec['Initial_Revenue_Type__c']==='SPCS') Pre_CO_Initial_List_Price_PSS_Extended__c+=nvl(rec['Comm_Man_Price__c']);
		if(rec['Recurring_Revenue_Type__c']==='SPCS') Pre_Carve_Out_List_Price_PSS_ExtendedA__c+=nvl(rec['Annual_list_Price__c']);
		if(rec['Initial_Revenue_Type__c']==='SPFP') Pre_Carve_Out_Sell_Price_PSS_FP_Deal__c+=nvl(rec['Sold_Value__c']);
		if(rec['Initial_Revenue_Type__c']==='SPTM') Pre_Carve_Out_Sell_Price_PSS_T_M_Deal__c+=nvl(rec['Sold_Value__c']);
		if(rec['Initial_Revenue_Type__c']==='SPCS') Pre_CO_Initial_Net_Price_PSS_Extended__c+=nvl(rec['Sold_Value__c']);
		if(rec['Recurring_Revenue_Type__c']==='SPCS') Pre_CO_Sell_Price_PSS_Ext_Support_Deal__c+=nvl(rec['Annual_Net_Price__c']);
		if(rec['Initial_Revenue_Type__c']==='SPFP') Pre_Carve_Out_List_Price_PS_T_M__c+=nvl(rec['Comm_Man_Price__c']);
//		if(rec['Initial_Revenue_Type__c']==='SPTM') Pre_Carve_Out_List_Price_PS_FP__c+=nvl(rec['Comm_Man_Price__c']);
		if(rec['Initial_Revenue_Type__c']==='SVFT') Pre_Carve_Out_List_Price_FED_T_M__c+=nvl(rec['Comm_Man_Price__c']);
		if(rec['Recurring_Revenue_Type__c']==='SVFT') Pre_C_O_List_Price_Recurring_FED_T_M__c+=nvl(rec['Annual_list_Price__c']);
		if(rec['Initial_Revenue_Type__c']==='SVFF') Pre_Carve_Out_List_Price_FED_FP__c+=nvl(rec['Comm_Man_Price__c']);
		if(rec['Recurring_Revenue_Type__c']==='SVFF') Pre_C_O_List_Price_Recurring_FED_FP__c+=nvl(rec['Annual_list_Price__c']);
		if(rec['Initial_Revenue_Type__c']==='SVTM') Pre_Carve_Out_Sell_Price_PS_T_M__c+=nvl(rec['Sold_Value__c']);
		if(rec['Initial_Revenue_Type__c']==='SVFP') Pre_Carve_Out_Sell_Price_PS_FP__c+=nvl(rec['Sold_Value__c']);
		if(rec['Initial_Revenue_Type__c']==='SVFT') Pre_Carve_OutvSell_Price_FED_T_M_Deal__c+=nvl(rec['Sold_Value__c']);
		if(rec['Recurring_Revenue_Type__c']==='SVFT') Pre_C_O_Sell_Price_Recurring_FED_T_M__c+=nvl(rec['Annual_Net_Price__c']);
		if(rec['Initial_Revenue_Type__c']==='SVFF') Pre_Carve_Out_Sell_Price_FED_FP_Deal__c+=nvl(rec['Sold_Value__c']);
		if(rec['Recurring_Revenue_Type__c']==='SVFF') Pre_C_O_Recurring_Sell_Price_FED_FP__c+=nvl(rec['Annual_Net_Price__c']);
		if(rec['Initial_Revenue_Type__c']==='ILF') Post_Carve_Out_Revenue_ILF__c+=nvl(rec['ILF_Fair_Value__c']);
		if(rec['Recurring_Revenue_Type__c']==='RLF') Post_Carve_Out_Revenue_RLF_from_ILF__c+=nvl(rec['Annual_Fair_Value__c']);
		if(rec['Initial_Revenue_Type__c']==='DVC1') Post_Carve_Rev_RA1_Deal__c+=nvl(rec['ILF_Fair_Value__c']);
		if(rec['Recurring_Revenue_Type__c']==='DVCS') Post_Carve_Rev_RLF_RA1_Deal__c+=nvl(rec['Annual_Fair_Value__c']);
		if(rec['Initial_Revenue_Type__c']==='DVF2') Post_Carve_Rev_RA2_Deal__c+=nvl(rec['ILF_Fair_Value__c']);
		if(rec['Recurring_Revenue_Type__c']==='DVT2') Post_Carve_Rev_RLF_from_RA2_annual_Deal__c+=nvl(rec['Annual_Fair_Value__c']);
		if(rec['Initial_Revenue_Type__c']==='Sub') Post_Carve_Rev_Subscription_one_off_Deal__c+=nvl(rec['ILF_Fair_Value__c']);
		if(rec['Recurring_Revenue_Type__c']==='Sub') Post_Carve_Rev_Subscription_Recurring__c+=nvl(rec['Annual_Fair_Value__c']);
		if(rec['Initial_Revenue_Type__c']==='DVCC') Post_Carve_Rev_RA1_Subscription_Deal__c+=nvl(rec['ILF_Fair_Value__c']);
		if(rec['Recurring_Revenue_Type__c']==='DVCS') Post_Carve_Rev_RA1_Recur_Subscription__c+=nvl(rec['Annual_Fair_Value__c']);
		if(rec['Initial_Revenue_Type__c']==='SPFP') Post_Carve_Rev_PSS_FP_Deal__c+=nvl(rec['ILF_Fair_Value__c']);
		if(rec['Initial_Revenue_Type__c']==='SPTM') Post_Carve_Rev_PSS_T_M_Deal__c+=nvl(rec['ILF_Fair_Value__c']);
		if(rec['Initial_Revenue_Type__c']==='SPCS') Post_Carve_Rev_PSS_Ext_Sup_Initial_Fee__c+=nvl(rec['ILF_Fair_Value__c']);
		if(rec['Recurring_Revenue_Type__c']==='SPCS') Post_Carve_Rev_PSS_Ext_Sup_annual_Deal__c+=nvl(rec['Annual_Fair_Value__c']);
		if(rec['Initial_Revenue_Type__c']==='SVTM') Post_Carve_Out_Revenue_PS_T_M__c+=nvl(rec['ILF_Fair_Value__c']);
		if(rec['Initial_Revenue_Type__c']==='SVFP') Post_Carve_Out_Revenue_PS_FP__c+=nvl(rec['ILF_Fair_Value__c']);
		if(rec['Initial_Revenue_Type__c']==='SVFT') Post_Carve_Rev_FED_T_M_Deal__c+=nvl(rec['ILF_Fair_Value__c']);
		if(rec['Recurring_Revenue_Type__c']==='SVFT') Post_Carve_Rev_FED_T_M_Recurring__c+=nvl(rec['Annual_Fair_Value__c']);
		if(rec['Initial_Revenue_Type__c']==='SVFF') Post_Carve_Rev_FED_FP_Deal__c+=nvl(rec['ILF_Fair_Value__c']);
		if(rec['Recurring_Revenue_Type__c']==='SVFF') Post_Carve_Rev_FED_FP_Recurring__c+=nvl(rec['Annual_Fair_Value__c']);
		if(rec['Initial_Revenue_Type__c']==='ILCS') Post_Carve_Rev_Packaged_Cloud_one_off__c+=nvl(rec['ILF_Fair_Value__c']);
		if(rec['Recurring_Revenue_Type__c']==='ILCS') Post_Carve_Rev_Package_Cloud_annual_Deal__c+=nvl(rec['Annual_Fair_Value__c']);
		if(rec['Recurring_Revenue_Type__c']==='Tran') Post_Carve_Rev_Packaged_Cloud_Transacti__c+=nvl(rec['Annual_Fair_Value__c']);
		if(rec['Initial_Revenue_Type__c']==='HOSF') Post_Carve_Rev_Cloud_Services_one_off__c+=nvl(rec['ILF_Fair_Value__c']);
		if(rec['Initial_Revenue_Type__c']==='ILF') Pre_Carve_Out_List_Price_ILF_Deal__c+=nvl(rec['Comm_Man_Price__c']);
		if(rec['Recurring_Revenue_Type__c']==='RLF') Pre_Carve_Out_List_Price_RLF_from_ILF__c+=rec['Recurring_Deal_Currency_List_Price__c'];
		if(rec['Initial_Revenue_Type__c']==='DVC1') Pre_Carve_Out_List_Price_RA1__c+=nvl(rec['Comm_Man_Price__c']);
		if(rec['Recurring_Revenue_Type__c']==='DVC1') Pre_Carve_Out_List_Price_Recurring_RA1__c+=rec['Recurring_Deal_Currency_List_Price__c'];
		if(rec['Initial_Revenue_Type__c']==='DVF2') Pre_Carve_Out_List_Price_RA2__c+=nvl(rec['Comm_Man_Price__c']);
		if(rec['Recurring_Revenue_Type__c']==='DVT2') Pre_Carve_Out_List_Price_RLF_from_RA2A__c+=nvl(rec['Annual_list_Price__c']);
		if(rec['Initial_Revenue_Type__c']==='Sub') Pre_Carve_Out_List_Price_Subscription_On__c+=nvl(rec['Comm_Man_Price__c']);
		if(rec['Recurring_Revenue_Type__c']==='Sub') Pre_Carve_Out_List_Price_Subscription_An__c+=nvl(rec['Annual_list_Price__c']);
		if(rec['Initial_Revenue_Type__c']==='DVCC') Pre_Carve_Out_List_Price_RA1_Sub__c+=nvl(rec['Comm_Man_Price__c']);
		if(rec['Recurring_Revenue_Type__c']==='DVCS') Pre_CO_List_Price_Recurring_Fee_RA1_Sub+=nvl(rec['Annual_list_Price__c']);
		if(rec['Initial_Revenue_Type__c']==='ILF') Pre_Carve_Out_Sell_Price_ILF__c+=nvl(rec['Sold_Value__c']);
		if(rec['Recurring_Revenue_Type__c']==='HOSS') Post_Carve_Rev_Cloud_Service_annual_Deal__c+=nvl(rec['Annual_Fair_Value__c']);
		if(rec['Initial_Revenue_Type__c']==='ILCS') Pre_CO_Sell_Price_Packaged_Cloud_One_off__c+=nvl(rec['Sold_Value__c']);
		if(rec['Recurring_Revenue_Type__c']==='ILCS') Pre_CO_Sell_Price_Packaged_Cloud_Annual__c+=nvl(rec['Annual_Net_Price__c']);
		if(rec['Recurring_Revenue_Type__c']==='Tran') Pre_CO_Sell_Price_Packaged_Cloud_Transac__c+=nvl(rec['Annual_Net_Price__c']);
		if(rec['Initial_Revenue_Type__c']==='ILCF') Pre_CO_SP_Cloud_Services_one_off_Deal__c+=nvl(rec['Sold_Value__c']);
		if(rec['Recurring_Revenue_Type__c']==='ILCS') Pre_Carve_OutSell_Price_Cloud_Services__c+=nvl(rec['Annual_Net_Price__c']);
		if(true) Initial_RPA_Total__c+=nvl(rec['Initial_RPA_Total__c']);
		if(rec['Initial_Revenue_Type__c']==='SVFP') Pre_Carve_Out_List_Price_PS_T_M__c+=nvl(rec['Comm_Man_Price__c']);
		Termination_Fee__c += nvl(rec['X3P_Termination_Fee__c']);
//		rec['SBQQ__DefaultSubscriptionTerm__c']=24;
		_debugRec(rec,'2.SBQQ__ListPrice__c '+rec['SBQQ__ListPrice__c']+' SBQQ__CustomerPrice__c '+rec['SBQQ__CustomerPrice__c']+' SBQQ__NetPrice__c '+rec['SBQQ__CustomerPrice__c']);

	});
	qre['Pre_Carve_Out_List_Price_PS_FP__c']=Pre_Carve_Out_List_Price_PS_FP__c;
	qre['Pre_Carve_Out_Sell_Price_RA1_Deal__c']=Pre_Carve_Out_Sell_Price_RA1_Deal__c;
	qre['Pre_Carve_Out_Sell_Price_Recurring_RA1__c']=Pre_Carve_Out_Sell_Price_Recurring_RA1__c;
	qre['Pre_Carve_Out_Sell_Price_RA2_Deal__c']=Pre_Carve_Out_Sell_Price_RA2_Deal__c;
	qre['Pre_CO_Sell_Price_RLF_from_RA2_annual__c']=Pre_CO_Sell_Price_RLF_from_RA2_annual__c;
	qre['Pre_Carve_Out_Sell_Price_Subs_Deal__c']=Pre_Carve_Out_Sell_Price_Subs_Deal__c;
	qre['Pre_Carve_Out_Sell_Price_Sub__c']=Pre_Carve_Out_Sell_Price_Sub__c;
	qre['Pre_COut_Sell_Price_RA1_Subs_Deal__c']=Pre_COut_Sell_Price_RA1_Subs_Deal__c;
	qre['Pre_CO_Sell_Price_Recurring_RA1_Subs__c']=Pre_CO_Sell_Price_Recurring_RA1_Subs__c;
	qre['Pre_Carve_Out_List_Price_Packaged_CloudO__c']=Pre_Carve_Out_List_Price_Packaged_CloudO__c;
	qre['Pre_Carve_Out_List_Price_Packaged_CloudA__c']=Pre_Carve_Out_List_Price_Packaged_CloudA__c;
	qre['Pre_Carve_Out_List_Price_Packaged_CloudT__c']=Pre_Carve_Out_List_Price_Packaged_CloudT__c;
	qre['Pre_Carve_Out_Sell_Price_RLF_from_ILF__c']=Pre_Carve_Out_Sell_Price_RLF_from_ILF__c;
	qre['Pre_Carve_Out_List_Price_Cloud_ServicesO__c']=Pre_Carve_Out_List_Price_Cloud_ServicesO__c;
	qre['Pre_Carve_Out_List_Price_Cloud_ServicesA__c']=Pre_Carve_Out_List_Price_Cloud_ServicesA__c;
	qre['Pre_Carve_Out_List_Price_PSS_FP__c']=Pre_Carve_Out_List_Price_PSS_FP__c;
	qre['Pre_Carve_Out_List_Price_PSS_T_M__c']=Pre_Carve_Out_List_Price_PSS_T_M__c;
	qre['Pre_CO_Initial_List_Price_PSS_Extended__c']=Pre_CO_Initial_List_Price_PSS_Extended__c;
	qre['Pre_Carve_Out_List_Price_PSS_ExtendedA__c']=Pre_Carve_Out_List_Price_PSS_ExtendedA__c;
	qre['Pre_Carve_Out_Sell_Price_PSS_FP_Deal__c']=Pre_Carve_Out_Sell_Price_PSS_FP_Deal__c;
	qre['Pre_Carve_Out_Sell_Price_PSS_T_M_Deal__c']=Pre_Carve_Out_Sell_Price_PSS_T_M_Deal__c;
	qre['Pre_CO_Initial_Net_Price_PSS_Extended__c']=Pre_CO_Initial_Net_Price_PSS_Extended__c;
	qre['Pre_CO_Sell_Price_PSS_Ext_Support_Deal__c']=Pre_CO_Sell_Price_PSS_Ext_Support_Deal__c;
	qre['Pre_Carve_Out_List_Price_PS_T_M__c']=Pre_Carve_Out_List_Price_PS_T_M__c;
	qre['Pre_Carve_Out_List_Price_PS_FP__c']=Pre_Carve_Out_List_Price_PS_FP__c;
	qre['Pre_Carve_Out_List_Price_FED_T_M__c']=Pre_Carve_Out_List_Price_FED_T_M__c;
	qre['Pre_C_O_List_Price_Recurring_FED_T_M__c']=Pre_C_O_List_Price_Recurring_FED_T_M__c;
	qre['Pre_Carve_Out_List_Price_FED_FP__c']=Pre_Carve_Out_List_Price_FED_FP__c;
	qre['Pre_C_O_List_Price_Recurring_FED_FP__c']=Pre_C_O_List_Price_Recurring_FED_FP__c;
	qre['Pre_Carve_Out_Sell_Price_PS_T_M__c']=Pre_Carve_Out_Sell_Price_PS_T_M__c;
	qre['Pre_Carve_Out_Sell_Price_PS_FP__c']=Pre_Carve_Out_Sell_Price_PS_FP__c;
	qre['Pre_Carve_OutvSell_Price_FED_T_M_Deal__c']=Pre_Carve_OutvSell_Price_FED_T_M_Deal__c;
	qre['Pre_C_O_Sell_Price_Recurring_FED_T_M__c']=Pre_C_O_Sell_Price_Recurring_FED_T_M__c;
	qre['Pre_Carve_Out_Sell_Price_FED_FP_Deal__c']=Pre_Carve_Out_Sell_Price_FED_FP_Deal__c;
	qre['Pre_C_O_Recurring_Sell_Price_FED_FP__c']=Pre_C_O_Recurring_Sell_Price_FED_FP__c;
	qre['Post_Carve_Out_Revenue_ILF__c']=Post_Carve_Out_Revenue_ILF__c;
	qre['Post_Carve_Out_Revenue_RLF_from_ILF__c']=Post_Carve_Out_Revenue_RLF_from_ILF__c;
	qre['Post_Carve_Rev_RA1_Deal__c']=Post_Carve_Rev_RA1_Deal__c;
	qre['Post_Carve_Rev_RLF_RA1_Deal__c']=Post_Carve_Rev_RLF_RA1_Deal__c;
	qre['Post_Carve_Rev_RA2_Deal__c']=Post_Carve_Rev_RA2_Deal__c;
	qre['Post_Carve_Rev_RLF_from_RA2_annual_Deal__c']=Post_Carve_Rev_RLF_from_RA2_annual_Deal__c;
	qre['Post_Carve_Rev_Subscription_one_off_Deal__c']=Post_Carve_Rev_Subscription_one_off_Deal__c;
	qre['Post_Carve_Rev_Subscription_Recurring__c']=Post_Carve_Rev_Subscription_Recurring__c;
	qre['Post_Carve_Rev_RA1_Subscription_Deal__c']=Post_Carve_Rev_RA1_Subscription_Deal__c;
	qre['Post_Carve_Rev_RA1_Recur_Subscription__c']=Post_Carve_Rev_RA1_Recur_Subscription__c;
	qre['Post_Carve_Rev_PSS_FP_Deal__c']=Post_Carve_Rev_PSS_FP_Deal__c;
	qre['Post_Carve_Rev_PSS_T_M_Deal__c']=Post_Carve_Rev_PSS_T_M_Deal__c;
	qre['Post_Carve_Rev_PSS_Ext_Sup_Initial_Fee__c']=Post_Carve_Rev_PSS_Ext_Sup_Initial_Fee__c;
	qre['Post_Carve_Rev_PSS_Ext_Sup_annual_Deal__c']=Post_Carve_Rev_PSS_Ext_Sup_annual_Deal__c;
	qre['Post_Carve_Out_Revenue_PS_T_M__c']=Post_Carve_Out_Revenue_PS_T_M__c;
	qre['Post_Carve_Out_Revenue_PS_FP__c']=Post_Carve_Out_Revenue_PS_FP__c;
	qre['Post_Carve_Rev_FED_T_M_Deal__c']=Post_Carve_Rev_FED_T_M_Deal__c;
	qre['Post_Carve_Rev_FED_T_M_Recurring__c']=Post_Carve_Rev_FED_T_M_Recurring__c;
	qre['Post_Carve_Rev_FED_FP_Deal__c']=Post_Carve_Rev_FED_FP_Deal__c;
	qre['Post_Carve_Rev_FED_FP_Recurring__c']=Post_Carve_Rev_FED_FP_Recurring__c;
	qre['Post_Carve_Rev_Packaged_Cloud_one_off__c']=Post_Carve_Rev_Packaged_Cloud_one_off__c;
	qre['Post_Carve_Rev_Package_Cloud_annual_Deal__c']=Post_Carve_Rev_Package_Cloud_annual_Deal__c;
	qre['Post_Carve_Rev_Packaged_Cloud_Transacti__c']=Post_Carve_Rev_Packaged_Cloud_Transacti__c;
	qre['Post_Carve_Rev_Cloud_Services_one_off__c']=Post_Carve_Rev_Cloud_Services_one_off__c;
	qre['Pre_Carve_Out_List_Price_ILF_Deal__c']=Pre_Carve_Out_List_Price_ILF_Deal__c;
	qre['Pre_Carve_Out_List_Price_RLF_from_ILF__c']=Pre_Carve_Out_List_Price_RLF_from_ILF__c;
	qre['Pre_Carve_Out_List_Price_RA1__c']=Pre_Carve_Out_List_Price_RA1__c;
	qre['Pre_Carve_Out_List_Price_Recurring_RA1__c']=Pre_Carve_Out_List_Price_Recurring_RA1__c;
	qre['Pre_Carve_Out_List_Price_RA2__c']=Pre_Carve_Out_List_Price_RA2__c;
	qre['Pre_Carve_Out_List_Price_RLF_from_RA2A__c']=Pre_Carve_Out_List_Price_RLF_from_RA2A__c;
	qre['Pre_Carve_Out_List_Price_Subscription_On__c']=Pre_Carve_Out_List_Price_Subscription_On__c;
	qre['Pre_Carve_Out_List_Price_Subscription_An__c']=Pre_Carve_Out_List_Price_Subscription_An__c;
	qre['Pre_Carve_Out_List_Price_RA1_Sub__c']=Pre_Carve_Out_List_Price_RA1_Sub__c;
	qre['Pre_CO_List_Price_Recurring_Fee_RA1_Sub__c']=Pre_CO_List_Price_Recurring_Fee_RA1_Sub__c;
	qre['Pre_Carve_Out_Sell_Price_ILF__c']=Pre_Carve_Out_Sell_Price_ILF__c;
	qre['Post_Carve_Rev_Cloud_Service_annual_Deal__c']=Post_Carve_Rev_Cloud_Service_annual_Deal__c;
	qre['Pre_CO_Sell_Price_Packaged_Cloud_One_off__c']=Pre_CO_Sell_Price_Packaged_Cloud_One_off__c;
	qre['Pre_CO_Sell_Price_Packaged_Cloud_Annual__c']=Pre_CO_Sell_Price_Packaged_Cloud_Annual__c;
	qre['Pre_CO_Sell_Price_Packaged_Cloud_Transac__c']=Pre_CO_Sell_Price_Packaged_Cloud_Transac__c;
	qre['Pre_CO_SP_Cloud_Services_one_off_Deal__c']=Pre_CO_SP_Cloud_Services_one_off_Deal__c;
	qre['Pre_Carve_OutSell_Price_Cloud_Services__c']=Pre_Carve_OutSell_Price_Cloud_Services__c;
	qre['Initial_RPA_Total__c']=Initial_RPA_Total__c;
	qre['Recurring_RPA_Total__c']=Recurring_RPA_Total__c;	
	qre['Pre_Carve_Out_List_Price_PS_T_M__c']=Pre_Carve_Out_List_Price_PS_T_M__c;
	qre['X3P_Termination_Fee__c']=Termination_Fee__c;
	if(nvl(qre['Quote_Initial_List_Total__c']) == 0)
		qre['Initial_RPA_Weighted__c'] = 0;
	else
		qre['Initial_RPA_Weighted__c'] = rnd(100*(1-nvl(qre['Initial_RPA_Total__c'])/qre['Quote_Initial_List_Total__c']));

	if(nvl(qre['Quote_Recurring_List_Total__c']) ==0)
		qre['Recurring_RPA_Weighted__c'] = 0;
	else
		qre['Recurring_RPA_Weighted__c'] = rnd(100*(1-nvl(qre['Recurring_RPA_Total__c'])/qre['Quote_Recurring_List_Total__c']));

}



function calcFairValue(aRev, quote, fvField,netField,rpaField)
{
	quote.leftOver = quote.net - quote.alpha;
	_debug('quote.leftOver='+quote.net+'-'+quote.alpha+'='+quote.leftOver);

	quote[fvField] =0;

	if(quote.leftOver<0 || quote.bResidual != true) //no residual or no money left over
	{
		_debug('No residuals or no money left over.');
		for(var sKey in aRev)
		{
			var rev = aRev[sKey];
			rev.arrResidual.forEach(function(rec) 
			{
				rec[fvField] = 0;
				_debugRec(rec,fvField+'(Residual)=0');					
			});	

			rev.FV = rev.alpha + (quote.net - quote.alpha)* rev.rpa/quote.rpa;	
			_debug('rev('+sKey+').'+fvField+'(nonResid)='+rev.alpha+'+('+quote.net+'-'+quote.alpha+')*'+rev.rpa+'/'+quote.rpa+'='+rev.FV);

			rev.nonResidual.forEach(function(rec)
			{
				rec[fvField] = (rev.rpa==0?0:(rev.FV * rec[rpaField]/rev.rpa));
				_debugRec(rec,fvField+'(nonResid)='+rev.FV+'*'+rec[rpaField]+'/'+rev.rpa+'='+rec[fvField]);					

			});
			quote[fvField]+=rev.FV;
		}
	}
	else
	{
		quote.beta = quote.leftOver - quote.net;
		_debug('quote.beta ='+quote.leftOver+'-'+quote.net+'='+quote.beta);
		if(quote.beta >= 0) 
		{
			_debug('The ammount of money left over great then the NP of the residual lines:');
			for(var sKey in aRev)
			{	
				var rev = aRev[sKey];		
				rev.arrResidual.forEach(function(rec) 
				{
					rec[fvField] = rec[netField];
					quote[fvField]+=rec[fvField];
					_debugRec(rec,fvField+'(Residual)='+rec[fvField]);					
				});
				_debug('Allocation the remaining to the non-residual revenue types:');
				rev.FV = rev.alpha + quote.beta * rev.rpaNonResid/quote.rpaNonResid;
				_debug('rev('+sKey+').'+fvField+'(nonResid)='+rev.alpha+'+'+quote.beta+'*'+rev.rpaNonResid+'/'+quote.rpaNonResid+'='+rev.FV);
				rev.nonResidual.forEach(function(rec)
				{
					rec[fvField] = rev.FV * rec[rpaField]/rev.rpaNonResid;
					quote[fvField]+=rec[fvField];	
					_debugRec(rec,fvField+'(nonResid)='+rev.FV +'*'+rec[rpaField]+'/'+rev.rpaNonResid+'='+rec[fvField]);	
				});
			}
		}
		else
		{
		
			for(var sKey in aRev)
			{	
				var rev = aRev[sKey];

				rev.fvResidual = quote.leftOver * rev.rpaResidual/quote.rpaResidual;
				_debug('How much of the remaining going to each residual line:');					
				_debug('rev('+sKey+').'+fvField+'(Residual)='+quote.leftOver+'*'+rev.rpaResidual+'/'+quote.rpaResidual+'='+rev.fvResidual);
				rev.arrResidual.forEach(function(rec) 
				{
					rec[fvField] = rev.fvResidual * rec[rpaField]/rev.rpaResidual;
					quote[fvField]+=rec[fvField];	
					_debugRec(rec,fvField+'(Residual)='+rev.fvResidual+'*'+rec[rpaField]+'/'+rev.rpaResidual+'='+rec[fvField]);					
				});
				_debug('All other revenue types FV is their a(rev):');
				rev.FV = rev.alpha;
				_debug('rev('+sKey+').'+fvField+'(nonResid)='+rev.FV);
				rev.nonResidual.forEach(function(rec)
				{
					rec[fvField] = rev.FV * rec[rpaField]/rev.rpaNonResid;
					quote[fvField]+=rec[fvField];	
					_debugRec(rec,fvField+'(nonResid)='+rev.FV+'*'+rec[rpaField]+'/'+rev.rpaNonResid+'='+rec[fvField]);							
				});
			}
		}
	}	
}

function _debugRec(rec,sMess)
{
	_debug( sMess+' ('+rec['SBQQ__ProductName__c']+','+rec['Id']+')');
}

var iDebug= 0;
var bDebug = true;
function _debug(sMess)
{
	if(bDebug)
	{
//		console.log(''+(iDebug++)+' '+sMess);
		console.log(sMess);
	}
}

function rnd(val)
{
	return Math.round(val*100)/100;
}
function nvl(val)
{
	if(val == null || isNaN(val))
		val=0;
	return val;
}

function copyPriceFields(line, record)
{
	var rec = line.record;
	rec['Initial_Pricebook_Price__c'] = record.Initial_Price_for_Tier__c;
	rec['Recurring_Pricebook_Price__c']= record.Recurring_Price_for_Tier__c;										
	rec['SBQQ__ListPrice__c'] = record.Recurring_Price_for_Tier__c;
	rec['Comm_Man_Price__c'] = record.Initial_Price_for_Tier__c;	
	rec['X3rd_Party_Cost_Type__c'] = 		record.X3rd_party_Cost_Type__c;							
	rec['Initial_3rd_party_Cost_List__c'] = record.Initial_3rd_party_Cost__c;	
	rec['ThirdParty_PO_Currency__c'] = record.Initial_Cost_Currency__c;	
	rec['Recurring_3rd_Party_Cost_List__c'] = record.Recurring_3rd_Party_Cost__c;	
	rec['Initial_Pricebook_Unit_Price__c'] = record.Initial_Unit_Price__c;	
	rec['Recurring_Pricebook_Unit_Price__c'] = record.Recurring_Unit_Price__c;	
	rec['Lower_Bound__c'] = record.Lower_Bound__c;	
}

function getTerm2(startDate, startDate2,endDate)
{
		var ret =1; 
		if(startDate == null)
			startDate = startDate2;
		if(startDate != null && endDate != null) 
		{ 
			ret = parseInt(endDate.substring(0,4))*12+parseInt(endDate.substring(5,7))-parseInt(startDate.substring(0,4))*12 - parseInt(startDate.substring(5,7)); 
			if(parseInt(endDate.substring(8,10))>=parseInt(startDate.substring(8,10))) 
				ret=ret+1; 
		}
		return ret/12; 

}
function getTerm(qre) 
{ 

var ret =1; 

var startDate = qre["SBQQ__StartDate__c"]; 
var endDate = qre["SBQQ__EndDate__c"]; 
if(startDate != null && endDate != null) 
{ 
	ret = parseInt(endDate.substring(0,4))*12+parseInt(endDate.substring(5,7))-parseInt(startDate.substring(0,4))*12 - parseInt(startDate.substring(5,7)); 
	if(parseInt(endDate.substring(8,10))>=parseInt(startDate.substring(8,10))) 
	ret=ret+1; 
}
return ret; 
}


function isNow(dateVal)
{
	var ret = true;
	if(dateVal == null || dateVal == '')
		ret=true;
	else
	{
		var dtNow = new Date();	
		var diff = parseInt(dateVal.substring(0,4))*12+parseInt(dateVal.substring(5,7))-
		(dtNow.getFullYear()*12+dtNow.getMonth()+1)
		if(diff!=0)
			ret = false;
		else
		ret = (parseInt(dateVal.substring(8,10))==dtNow.getDate());	
	}
	_debug('isNow '+ dateVal+ ' ret ='+ret);
	return ret;
}

function getTermRatio(rec,qre)
{

    var ret =1;
    var term = null;
    var dtNow = new Date();
	var startDate = rec["Subs_Start_Date__c"];
	if(startDate == null)
		startDate = qre["SBQQ__StartDate__c"];
	if(startDate == null)
		throw ('Please specify Start Date');

	var endDate = rec["SBQQ__EndDate__c"];
	if(endDate == null)
		endDate = qre["SBQQ__EndDate__c"];
	if(endDate != null)
	{
		term = parseInt(endDate.substring(0,4))*12+parseInt(endDate.substring(5,7))-parseInt(startDate.substring(0,4))*12 - parseInt(startDate.substring(5,7));
		if(parseInt(endDate.substring(8,10))>=parseInt(startDate.substring(8,10)))
	  		term=term+1;
	}
	if(term == null)
		term = qre["SBQQ__SubscriptionTerm__c"];
	if(term == null)
		throw ('Please specify Subscription Term');
	var term2 =dtNow.getFullYear()*12+dtNow.getMonth()+1-parseInt(startDate.substring(0,4))*12 - parseInt(startDate.substring(5,7));
		if(dtNow.getDate()>=parseInt(startDate.substring(8,10)))
	  		term2=term2+1;
	var ret = (term-term2)/term;
	if(ret<0)
		ret =0;
	if(ret > 1)
		ret = 1;
	_debug('getTermRatio = '+ret+'=('+term+'-'+term2+')/'+term);
	return ret;
}


export function onAfterCalculate(quote, lines) 
{
		if (lines.length) 
	{

		lines.forEach(function(line) 
		{
			var rec = line.record;
			rec['SBQQ__NetTotal__c'] = rec['Recurring_Net_Total__c'];
			rec['SBQQ__CustomerPrice__c'] =  rec['Recurring_Net_Total__c'];
			rec['SBQQ__NetPrice__c'] =  rec['Recurring_Net_Total__c'];			
		});
	}
	return Promise.resolve();
};