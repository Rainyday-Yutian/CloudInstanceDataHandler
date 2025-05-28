# coding: utf-8
import pandas as pd
import json,requests
import datetime,time,os
from pprint import pprint 
import numpy as np

from aliyunsdkcore.client import AcsClient
# from aliyunsdkcore.acs_exception.exceptions import ClientException
# from aliyunsdkcore.acs_exception.exceptions import ServerException
# from aliyunsdkcore.auth.credentials import StsTokenCredential
from aliyunsdkcore.auth.credentials import AccessKeyCredential
from aliyunsdkcms.request.v20190101.DescribeMetricDataRequest import DescribeMetricDataRequest
from aliyunsdkcms.request.v20190101.DescribeMetricLastRequest import DescribeMetricLastRequest
from aliyunsdkcms.request.v20190101.DescribeMetricListRequest import DescribeMetricListRequest
from aliyunsdkecs.request.v20140526.DescribeInstancesRequest import DescribeInstancesRequest
from aliyunsdkvpc.request.v20160428.DescribeEipAddressesRequest import DescribeEipAddressesRequest
from aliyunsdkecs.request.v20140526.DescribeNetworkInterfacesRequest import DescribeNetworkInterfacesRequest
from aliyunsdkslb.request.v20140515.DescribeLoadBalancersRequest import DescribeLoadBalancersRequest
from aliyunsdkvpc.request.v20160428.DescribeIpv6AddressesRequest import DescribeIpv6AddressesRequest
from aliyunsdkr_kvstore.request.v20150101.DescribeInstancesOverviewRequest import DescribeInstancesOverviewRequest
from aliyunsdkr_kvstore.request.v20150101.DescribeInstancesRequest import DescribeInstancesRequest as DescribeRedisInsRequest
from aliyunsdkvpc.request.v20160428.DescribeCommonBandwidthPackagesRequest import DescribeCommonBandwidthPackagesRequest
from aliyunsdkvpc.request.v20160428.DescribeNatGatewaysRequest import DescribeNatGatewaysRequest
from aliyunsdkrds.request.v20140815.DescribeDBInstancesRequest import DescribeDBInstancesRequest
from aliyunsdkdds.request.v20151201.DescribeDBInstancesRequest import DescribeDBInstancesRequest
from aliyunsdkecs.request.v20140526.DescribeDisksRequest import DescribeDisksRequest

from aliyunsdkcore.request import CommonRequest

# 忘记这是什么需求了
import inspect
def printali(message):
    # 获取调用者的模块名
    module_name = inspect.getmodule(inspect.currentframe().f_back).__name__
    print(f"[{module_name}]: {message}")

try:
    import oss2
    from oss2.credentials import StaticCredentialsProvider
except:
    printali("导入oss2失败，相关功能将不可用!\n")

# 云监控通用 #
def getTimeDict(start_offset_days=0,end_offset_days=0,start_offset_hours=0,end_offset_hours=0,start_offset_minutes=0,end_offset_minutes=0,start_datetime=None,end_datetime=None):
    """
    此方法基于基底时间和偏移量参数来生成时间范围字典： 起始时间或结束时间 = 基底时间 - 偏移量。
    当end_datetime未被指定时，默认使用当前时间作为基底时间进行偏移计算。
    当start_datetime未被指定时，默认使用end_datetime(此处指已被偏移过的end_datetime)作为基底时间进行偏移计算；
    即起始时间和结束时间如果都未被指定，则默认使用当前时间作为基底时间进行偏移计算。
    当end_datetime和start_datetime都被指定时，则各自分别作为起始时间和结束时间的基底时间。
    """
    if end_datetime is None:
        end_datetime = datetime.datetime.now()
    end_datetime = end_datetime - datetime.timedelta(days=end_offset_days,hours=end_offset_hours,minutes=end_offset_minutes)
    if start_datetime:
        start_datetime = start_datetime - datetime.timedelta(days=start_offset_days,hours=start_offset_hours,minutes=start_offset_minutes)
    else:
        start_datetime = end_datetime - datetime.timedelta(days=start_offset_days,hours=start_offset_hours,minutes=start_offset_minutes)
    start_datetime.replace(second=0)
    end_datetime.replace(second=0)
    if start_datetime > end_datetime:
        raise ValueError("Start time must be earlier than End time!")
    if end_datetime-start_datetime > datetime.timedelta(days=60):
        print("Warnings: Time range is too long!!")
    TimeDict = {
        "start_datetime":start_datetime,
        "end_datetime":end_datetime,
        "start_timestamp":int(start_datetime.timestamp()),
        "end_timestamp":int(end_datetime.timestamp()),
        "start_timestring":start_datetime.strftime('%Y-%m-%d %H:%M:%S'),
        "end_timestring":end_datetime.strftime('%Y-%m-%d %H:%M:%S'),
        "start_timestring_iso8601":start_datetime.strftime("%Y-%m-%dT%H:%M:%S")+"+08:00",
        "end_timestring_iso8601":end_datetime.strftime("%Y-%m-%dT%H:%M:%S")+"+08:00"
        }
    print(f"{TimeDict['start_timestring']} -> {TimeDict['end_timestring']}")
    return TimeDict

# generate_timestamps函数用于生成分页查询的时间戳，以下代码由通义千问生成，仅作参考，后续会完善
def generate_timestamps(start_timestamp, end_timestamp, period, max_length, instance_count):
    total_query_range = end_timestamp - start_timestamp
    query_range_per_instance = (max_length // instance_count) * period
    page_count = -(-total_query_range // query_range_per_instance)  # 向上取整

    timestamps = []
    current_start = start_timestamp
    for _ in range(page_count):
        current_end = min(current_start + query_range_per_instance, end_timestamp)
        timestamps.append((current_start, current_end))
        current_start = current_end

    return timestamps

    # 示例参数
    start_timestamp = int(datetime.datetime(2023, 10, 1, 0, 0, 0).timestamp())
    end_timestamp = int(datetime.datetime(2023, 10, 2, 0, 0, 0).timestamp())
    period = 300  # 5分钟
    max_length = 1440
    instance_count = 50

    # 生成分页查询的时间戳
    timestamps = generate_timestamps(start_timestamp, end_timestamp, period, max_length, instance_count)

    # 打印分页查询的时间戳
    for i, (start, end) in enumerate(timestamps):
        print(f"Page {i+1}: Start Timestamp: {start}, End Timestamp: {end}")

# =========================================================================================================================== #

__CurrentPath__ = os.path.dirname(os.path.realpath(__file__)) + "/"
__DataPath__ = __CurrentPath__ + "../data/"

# TODO: 考虑移除各子类方法中自定义添加的self_RegionId，或者指定好要移除的参数
class BasicDataFrame():
    def __init__(self)-> None:
        self.InsType = self.__class__.__name__
        self.Prefix = "Aliyun"
        self.InsInfo:pd.DataFrame = pd.DataFrame()
        self.MetricData:pd.DataFrame = pd.DataFrame()
        self.InsData:pd.DataFrame = pd.DataFrame()

    def statisticMetricData()-> pd.DataFrame:
        pass

    def saveInsInfo(self,one_sheet=True,rename_suffix=None,Path=__DataPath__,split_by="self_RegionId")-> None:
        if rename_suffix:
            filename = Path + f'{self.Prefix}_{self.InsType}_Info_{rename_suffix}.xlsx'
        else:
            filename = Path + f'{self.Prefix}_{self.InsType}_Info.xlsx'
        print("Exporting InsInfo to Excel: ", filename)
        writer = pd.ExcelWriter(filename)
        if one_sheet:
            self.InsInfo.to_excel(writer,sheet_name="ALL",index=False)
        elif one_sheet == False and split_by in self.InsInfo.columns:
            for sample in self.InsInfo[split_by].unique():
                df_info = self.InsInfo[self.InsInfo[split_by] == sample]
                df_info.to_excel(writer,sheet_name=sample,index=False)
        else:
            # raise ValueError("split_by must be in InsInfo.columns")
            print(f"split_by '{split_by}' must be in InsInfo.columns, saveing to one sheet!")
            self.InsInfo.to_excel(writer,sheet_name="ALL",index=False)
        writer.close()
        
    def saveInsData(self,one_sheet=True,rename_suffix="Data",Path=__DataPath__,split_by="self_RegionId")-> None:
        filename = Path + f'{self.Prefix}_{self.InsType}_{rename_suffix}.xlsx'
        print("Exporting InsData to Excel: ", filename)
        writer = pd.ExcelWriter(filename)
        if one_sheet:
            self.InsData.to_excel(writer,sheet_name="All",index=False)
        elif one_sheet == False and split_by in self.InsData.columns:
            for sample in self.InsData[split_by].unique():
                df_data = self.InsData[self.InsData[split_by] == sample]
                df_data.to_excel(writer,sheet_name=sample,index=False)
        writer.close()

    def saveOtherData(self,df,rename_suffix="MoreInfo",format="xlsx",Path=__DataPath__)-> None:
        if format == "xlsx":
            filename = Path + f'{self.Prefix}_{self.InsType}_{rename_suffix}.xlsx'
            print(f"Exporting {rename_suffix} Data to Excel: ", filename)
            df.to_excel(filename,sheet_name="All",index=False)
        elif format == "csv":
            filename = Path + f'{self.Prefix}_{self.InsType}_{rename_suffix}.csv'
            print(f"Exporting {rename_suffix} Data to CSV: ", filename)
            df.to_csv(filename,index=False)
        else:
            raise ValueError("Format must be 'csv' or 'xlsx'")

    def saveAll(self)-> None:
        self.saveInsInfo()
        self.saveInsData()

    def Filter(self,df,field,string,operator="contain"):
        pass

    def genDimension(self,df) -> list:
        # Dimensions_Disk = Test.InsInfo[['InstanceId', 'device_format']].to_dict('records')
        pass

class AliyunInstance(BasicDataFrame):
    # v1.0.6
    # def __init__(self,credentials)-> None:
    #     super().__init__()
    #     self.InsType = self.__class__.__name__
    #     self.Credentials = credentials
    #     self.Namespace:str = None
    #     self.ProductCategory = None
    #     self.Dimensions:str = 'instanceId'
    
    # v1.1.0+ 关于credentials，后期版本考虑移除self.Credentials，当有实际请求时再传入self.ak,self.sk来构建，构造后有效期应为31分钟，华为则更短
    def __init__(self,ak,sk)-> None:
        # self.AK = ak
        # self.SK = sk
        self.InsType = self.__class__.__name__
        self.Credentials = AccessKeyCredential(ak, sk)
        self.Namespace:str = None
        self.ProductCategory = None
        self.Dimensions:str = 'instanceId'

    def getMetricList(self,instance_list:list,metric_name:str,TimeDict:object,period:str="300",statistic="Maximum",StatisticsApproach:list=['max'],GroupBy:str='instanceId',Dimensions:list=None,DisplayMetricName=None,region_id:str="cn-hangzhou",page_size:int=50,sleep_time=0) -> pd.DataFrame:
        # 合法性检查
        duration_time = TimeDict["end_datetime"] - TimeDict["start_datetime"]
        if duration_time > datetime.timedelta(days=60) or page_size > 50 or page_size < 1:
            raise ValueError("Duration time must be less than 60 days, and page size must be between 1 and 50")
        
        allowed_statistics = ["Value","Maximum","Minimum","Average","Sum"]
        if statistic not in allowed_statistics:
            raise ValueError(f"Invalid statistic value '{statistic}'. Allowed values are: {', '.join(allowed_statistics)}")
        
        # Dimension需要重写，构造Dimensions时，可以参考Dataframe，先铺平
        # GroupBy需要重写，GroupBy改为列表，允许多个维度
        # if GroupBy not in [self.Dimensions,'timestamp',None]:
        #     raise ValueError(f"Invalid GroupBy value '{GroupBy}'. Allowed values are: {', '.join([self.Dimensions,'timestamp','obj:None'])}")
        allowed_StatisticsApproach = ['last', 'max', 'min', 'avg', 'max_95','sum','all','raw']
        if 'all' in StatisticsApproach:
            StatisticsApproach = allowed_StatisticsApproach[:-1]
        elif 'raw' in StatisticsApproach or StatisticsApproach == [] or StatisticsApproach is None:
            StatisticsApproach = None
        elif not set(StatisticsApproach).issubset(set(allowed_StatisticsApproach)):
            raise ValueError(f"Invalid StatisticsApproach value(s): {set(StatisticsApproach) - set(allowed_StatisticsApproach)}. Allowed values are: {', '.join(allowed_StatisticsApproach)}")
        
        if DisplayMetricName is None:
            DisplayMetricName = metric_name

        if Dimensions is None:
            dimensions_list = [{self.Dimensions: instanceId} for instanceId in instance_list]
        else:
            dimensions_list = Dimensions

        client = AcsClient(region_id=region_id, credential=self.Credentials)
        request = DescribeMetricListRequest()
        request.set_accept_format('json')
        request.set_Namespace(self.Namespace)
        request.set_MetricName(metric_name)
        request.set_Period(period)
        request.set_StartTime(TimeDict["start_timestring"])
        request.set_EndTime(TimeDict["end_timestring"])
        df_data = pd.DataFrame()
        for idx in range(0, len(dimensions_list),page_size):
            batch_dimensions_list = dimensions_list[idx:idx+page_size]
            request.set_Dimensions(batch_dimensions_list)    
            response_json = json.loads(client.do_action_with_exception(request))
            df_data_temp = pd.DataFrame(json.loads(response_json['Datapoints']))
            print(df_data_temp)
            df_data = pd.concat([df_data, df_data_temp])
            time.sleep(sleep_time)
        
        if statistic not in df_data.columns:
            for stat in allowed_statistics:
                if stat in df_data.columns:
                    statistic = stat
                    break
            
        aggregation_functions = {}
        # 这个判断后加的，先这样
        if df_data.empty:
            print("No data found,pls check request params, return a empty dataframe!")
            return df_data

        if StatisticsApproach:
            if "last" in StatisticsApproach:
                aggregation_functions[f'{DisplayMetricName}_last'] = (f'{statistic}', lambda x: x.iloc[x.index.get_loc(x.idxmax())])
            if "max" in StatisticsApproach:
                aggregation_functions[f'{DisplayMetricName}_max'] = (f'{statistic}', 'max')
            if "min" in StatisticsApproach:
                aggregation_functions[f'{DisplayMetricName}_min'] = (f'{statistic}', 'min')
            if "avg" in StatisticsApproach:
                aggregation_functions[f'{DisplayMetricName}_avg'] = (f'{statistic}', 'mean')
            if "max_95" in StatisticsApproach:
                aggregation_functions[f'{DisplayMetricName}_max_95'] = (f'{statistic}', lambda x: x.quantile(0.95))
            if "sum" in StatisticsApproach:
                aggregation_functions[f'{DisplayMetricName}_sum'] = (f'{statistic}', 'sum')
            # if "raw" in StatisticsApproach:
            #     aggregation_functions[f'{DisplayMetricName}'] = (f'{metric_name}', 'list')
            if GroupBy:
                df_data = df_data.groupby(GroupBy).agg(**aggregation_functions).reset_index()
            else:
                df_data = df_data.agg(**aggregation_functions).reset_index()
        else:
            print('Parmas StatisticsApproach has contain "raw", return raw data!')
        return df_data
    
    def getMetricData(self,instance_list:list,metric_name:str,TimeDict:object,period:str="300",statistic="Maximum",StatisticsApproach:list=['max'],GroupBy:str='instanceId',Dimensions:list=None,DisplayMetricName=None,region_id:str="cn-hangzhou",page_size:int=50,sleep_time=0) -> pd.DataFrame:
        """
        v1.1.0+ 
        getMetricData参数命名规范
        * 为避免参数命名冲突及管理混乱，接口原生支持的参数使用小写命名，如period、statistic等，函数自定义功能参数使用驼峰命名，如StatisticsApproach、DisplayMetricName等

        metric_name:
        阿里云云监控官方查询地址 https://cms.console.aliyun.com/metric-meta/acs_ecs_dashboard/ecs
        
        statistics: "Maximum","Minimum","Average","Value","Sum" 其一
        阿里云API定义的statistics参数 表示请求种指定的period参数为统计周期，以Dimensions为维度分组，对值进行statistics所指定方式进行统计
        API一般直接返回这三种组合的数据列：["Maximum","Minimum","Average"]、["Value"]、["Sum"]
        这里的statistics其实不作为API官方参数，只是函数目前只支持对一列数据进行统计，需要此参数作为StatisticsApproach（统计方法）的数据依据列
        具体指标返回哪种组合数据，请根据上面的官方地址自行查询

        StatisticsApproach: ["last", "max", "min", "avg", "max_95", "sum", "raw"] 的子集
        本函数定义的StatisticsApproach参数，表示方法中指定的TimeDict参数为统计周期，以GroupBy进行分组，对值进行StatisticsApproach指定方式统计
            last: TimeDict指定的时间范围内，以GroupBy进行分组统计，取最新的值。
            max、min、avg: TimeDict指定的时间范围内，以GroupBy进行分组统计，取最大值、最小值、平均值。
            max_95: TimeDict指定的时间范围内，以GroupBy进行分组统计，取95%分位数。
            all: TimeDict指定的时间范围内，以GroupBy进行分组统计，统计以上所有值。
            raw/[]/None: 不进行分组统计，直接返回原始数据，此时GroupBy参数无效。

        GroupBy: "instanceId","timestamp",None 其一
            当StatisticsApproach为raw/[]/None时，GroupBy参数无效，表示返回原始数据，无需分组统计
        """


        # 合法性检查
        duration_time = TimeDict["end_datetime"] - TimeDict["start_datetime"]
        if duration_time > datetime.timedelta(days=60) or page_size > 50 or page_size < 1:
            raise ValueError("Duration time must be less than 60 days, and page size must be between 1 and 50")
        
        allowed_statistics = ["Value","Maximum","Minimum","Average","Sum"]
        if statistic not in allowed_statistics:
            raise ValueError(f"Invalid statistic value '{statistic}'. Allowed values are: {', '.join(allowed_statistics)}")
        
        # Dimension需要重写，构造Dimensions时，可以参考Dataframe，先铺平
        # GroupBy需要重写，GroupBy改为列表，允许多个维度
        # if GroupBy not in [self.Dimensions,'timestamp',None]:
        #     raise ValueError(f"Invalid GroupBy value '{GroupBy}'. Allowed values are: {', '.join([self.Dimensions,'timestamp','obj:None'])}")
        allowed_StatisticsApproach = ['last', 'max', 'min', 'avg', 'max_95','sum','all','raw']
        if 'all' in StatisticsApproach:
            StatisticsApproach = allowed_StatisticsApproach[:-1]
        elif 'raw' in StatisticsApproach or StatisticsApproach == [] or StatisticsApproach is None:
            StatisticsApproach = None
        elif not set(StatisticsApproach).issubset(set(allowed_StatisticsApproach)):
            raise ValueError(f"Invalid StatisticsApproach value(s): {set(StatisticsApproach) - set(allowed_StatisticsApproach)}. Allowed values are: {', '.join(allowed_StatisticsApproach)}")
        
        if DisplayMetricName is None:
            DisplayMetricName = metric_name

        if Dimensions is None:
            dimensions_list = [{self.Dimensions: instanceId} for instanceId in instance_list]
        else:
            dimensions_list = Dimensions

        client = AcsClient(region_id=region_id, credential=self.Credentials)
        request = DescribeMetricDataRequest()
        request.set_accept_format('json')
        request.set_Namespace(self.Namespace)
        request.set_MetricName(metric_name)
        request.set_Period(period)
        request.set_StartTime(TimeDict["start_timestring"])
        request.set_EndTime(TimeDict["end_timestring"])
        # request.set_Express(f'{{"groupby":[{self.Dimensions},"timestamp"]}}')
        # request.set_Length('1440')
        # 阿里云接口最新不止支持到1440，后续有需求再编写分页查询，参考 generate_timestamps()
        # print(TimeDict['start_timestamp'])
        df_data = pd.DataFrame()
        for idx in range(0, len(dimensions_list),page_size):
            batch_dimensions_list = dimensions_list[idx:idx+page_size]
            request.set_Dimensions(batch_dimensions_list)    
            response_json = json.loads(client.do_action_with_exception(request))
            df_data_temp = pd.DataFrame(json.loads(response_json['Datapoints']))
            df_data = pd.concat([df_data, df_data_temp])
            time.sleep(sleep_time)
        
        if statistic not in df_data.columns:
            for stat in allowed_statistics:
                if stat in df_data.columns:
                    statistic = stat
                    break
            
        aggregation_functions = {}
        # 这个判断后加的，先这样
        if df_data.empty:
            print("No data found,pls check request params, return a empty dataframe!")
            return df_data

        if StatisticsApproach:
            if "last" in StatisticsApproach:
                aggregation_functions[f'{DisplayMetricName}_last'] = (f'{statistic}', lambda x: x.iloc[x.index.get_loc(x.idxmax())])
            if "max" in StatisticsApproach:
                aggregation_functions[f'{DisplayMetricName}_max'] = (f'{statistic}', 'max')
            if "min" in StatisticsApproach:
                aggregation_functions[f'{DisplayMetricName}_min'] = (f'{statistic}', 'min')
            if "avg" in StatisticsApproach:
                aggregation_functions[f'{DisplayMetricName}_avg'] = (f'{statistic}', 'mean')
            if "max_95" in StatisticsApproach:
                aggregation_functions[f'{DisplayMetricName}_max_95'] = (f'{statistic}', lambda x: x.quantile(0.95))
            if "sum" in StatisticsApproach:
                aggregation_functions[f'{DisplayMetricName}_sum'] = (f'{statistic}', 'sum')
            # if "raw" in StatisticsApproach:
            #     aggregation_functions[f'{DisplayMetricName}'] = (f'{metric_name}', 'list')
            if GroupBy:
                df_data = df_data.groupby(GroupBy).agg(**aggregation_functions).reset_index()
            else:
                df_data = df_data.agg(**aggregation_functions).reset_index()
        else:
            print('Parmas StatisticsApproach has contain "raw", return raw data!')
        return df_data
    
    def getMetricLast_NextToken(self,region_id,instance_list,metric_name,Period="60",TimeDict=None,DisplayMetricName=None,page_size=50) -> pd.DataFrame:
        if DisplayMetricName is None:
            DisplayMetricName = metric_name
        client = AcsClient(region_id=region_id, credential=self.Credentials)
        # request = DescribeMetricListRequest()
        request = DescribeMetricLastRequest()
        request.set_accept_format('json')
        request.set_MetricName(metric_name)
        request.set_Period(Period)
        request.set_StartTime(TimeDict["start_timestring"])
        request.set_EndTime(TimeDict["end_timestring"])
        request.set_Namespace(self.Namespace)
        request.set_Express('{"groupby":["userId","instanceId"]}')
        request.set_Length("1000")
        df_data = pd.DataFrame()
        dimensions_list = [{"instanceId": instanceId} for instanceId in instance_list]
        request.set_Dimensions(dimensions_list)
        response = client.do_action_with_exception(request)
        response_json = json.loads(response)
        if response_json['Success']:
            df_data = pd.concat([df_data, pd.DataFrame(json.loads(response_json['Datapoints']))])
            NextToken = response_json.get("NextToken")
            while(NextToken):
                request.set_NextToken(NextToken)
                df_data = pd.concat([df_data, pd.DataFrame(json.loads(response_json['Datapoints']))])
        else:
            pprint(response)
            print("Failed to obtain the data point!Return a empty dataframe!")
            df_data = pd.DataFrame()
        df_data.rename(columns={col: f'{DisplayMetricName}_{col}' for col in df_data.columns if col not in ['instanceId','timestamp','userId']}, inplace=True)
        return df_data
    
    # 子类自行实现适配
    # def getInsInfo():
    #     pass

    # def getInsData():
    #     pass

class ECS(AliyunInstance):
    def __init__(self,Credentials)-> None:
        super().__init__(Credentials)
        self.Namespace = "acs_ecs_dashboard"
        self.ProductCategory = "ecs"
        self.ENIsFromAllECS = None

    # v1.0.6 这个没写好，需要重新写
    def getECSInfo(self,region_id,instance_list=None,instance_name=None,page_size=100) -> pd.DataFrame:
        client = AcsClient(region_id=region_id, credential=self.Credentials)
        request = DescribeInstancesRequest()
        request.set_accept_format('json')
        request.set_PageSize(page_size)
        request.set_PageNumber(1)
        if instance_list is None:
            if instance_name is not None:
                request.set_InstanceName(instance_name)
            response_json = json.loads(client.do_action_with_exception(request))
            df = pd.DataFrame(response_json['Instances']['Instance'])
            page_total = response_json['TotalCount'] // page_size + 1
            for page_number in range(2,page_total+1):
                request.set_PageNumber(page_number)
                response_json = json.loads(client.do_action_with_exception(request))
                df_clip = pd.DataFrame(response_json['Instances']['Instance'])
                df = pd.concat([df, df_clip],ignore_index=True)
        else:
            df = pd.DataFrame()
            for idx in range(0, len(instance_list),page_size):
                batch_inslist = instance_list[idx:idx+page_size]
                request.set_InstanceIds(batch_inslist)
                response_json = json.loads(client.do_action_with_exception(request))
                df_clip = pd.DataFrame(response_json['Instances']['Instance'])
                df = pd.concat([df, df_clip],ignore_index=True)
        df['self_RegionId'] = region_id
        self.InsInfo = pd.concat([self.InsInfo,df],axis=0,ignore_index=True)
        return df
    
    def extractEIPFromInsInfo(self):
        df_ENI = pd.DataFrame()
        if self.InsInfo.empty:
            print(f'No {self.InsType} Info Data Found...')
        else:
            df = self.InsInfo
            df['NetworkInterfaces'] = df['NetworkInterfaces'].apply(lambda x: x['NetworkInterface'])
            for index,row in df.iterrows():
                df_temp = pd.DataFrame(row['NetworkInterfaces'])
                df_temp['InstanceName'] = row['InstanceName']
                df_temp['InstanceId'] = row['InstanceId']
                df_temp['RegionId'] = row['RegionId']
                df_ENI = pd.concat([df_ENI, df_temp],ignore_index=True)
            # print(f'Extracted EIP from {self.InsType} Data...')
        self.ENIsFromAllECS = df_ENI
        return df_ENI
    

class Disks(AliyunInstance):
    def __init__(self,Credentials)-> None:
        super().__init__(Credentials)
        self.Namespace = "acs_ecs_dashboard"
        self.ProductCategory = "ecs"

    def getInsInfo(self,region_id,disk_ids_list=None,instance_id:str=None,instance_name=None,disk_type="all",page_size=100) -> pd.DataFrame:
        if disk_type not in ['all','system','data']:
            raise ValueError('disk_type must be "all","system","data"')
        client = AcsClient(region_id=region_id, credential=self.Credentials)
        request = DescribeDisksRequest()
        request.set_accept_format('json')
        request.set_DiskType(disk_type)
        request.set_PageSize(page_size)
        request.set_PageNumber(1)
        if instance_id:
            request.set_InstanceId(instance_id)
        if disk_ids_list:
            request.set_DiskIds(disk_ids_list)
        response_json = json.loads(client.do_action_with_exception(request))
        df = pd.DataFrame(response_json['Disks']['Disk'])
        page_total = response_json['TotalCount'] // page_size + 1
        for page_number in range(2,page_total+1):
            request.set_PageNumber(page_number)
            response_json = json.loads(client.do_action_with_exception(request)) 
            df_clip = pd.DataFrame(response_json['Disks']['Disk'])
            df = pd.concat([df, df_clip],ignore_index=True)
        self.InsInfo = pd.concat([self.InsInfo,df],axis=0,ignore_index=True)
        return df
        # 2025.2.27 阿里云这个接口的新版分页方式好像还未支持好，先弃用新版分页方式
        # request.set_MaxResults(page_size)
        # response_json = json.loads(client.do_action_with_exception(request))
        # df = pd.DataFrame(response_json['Disks']['Disk'])
        # while(response_json.get('NextToken') is not None):
        #     request.set_NextToken(response_json['NextToken'])
        #     response_json = json.loads(client.do_action_with_exception(request))
        #     pprint(response_json)
        #     time.sleep(1)
        #     df_clip = pd.DataFrame(response_json['Disks']['Disk'])
        #     df = pd.concat([df, df_clip],ignore_index=True)
        # self.InsInfo = pd.concat([self.InsInfo,df],axis=0,ignore_index=True)
        # return df
    
class ENI(AliyunInstance):
    def __init__(self,Credentials)-> None:
        super().__init__(Credentials)
        
    # def getENIInfo(self,region_id,instance_list=None,page_size=100):
    #     client = AcsClient(region_id=region_id, credential=self.Credentials)
    #     request = DescribeNetworkInterfacesRequest()
    #     request.set_accept_format('json')
    #     request.set_PageSize(page_size)
    #     request.set_PageNumber(1)
    #     df = pd.DataFrame()
    #     if instance_list:
    #         for idx in range(0, len(instance_list),page_size):
    #             request.set_NetworkInterfaceIds(instance_list[idx:idx+page_size])
    #             response_json = json.loads(client.do_action_with_exception(request))
    #             df_clip = pd.DataFrame(response_json['NetworkInterfaceSets']['NetworkInterfaceSet'])
    #             df = pd.concat([df, df_clip],ignore_index=True)
    #     else:
    #         request.set_PageNumber(1)
    #         response_json = json.loads(client.do_action_with_exception(request))
    #         df = pd.DataFrame(response_json['NetworkInterfaceSets']['NetworkInterfaceSet']) 
    #         page_total = response_json['TotalCount'] // page_size + 1
    #         for page_number in range(2,page_total+1):
    #             request.set_PageNumber(page_number)
    #             response_json = json.loads(client.do_action_with_exception(request))
    #             df_clip = pd.DataFrame(response_json['NetworkInterfaceSets']['NetworkInterfaceSet'])
    #             df = pd.concat([df, df_clip],ignore_index=True) 
    #     df['self_RegionId'] = region_id
    #     self.InsInfo = pd.concat([self.InsInfo,df],axis=0,ignore_index=True)
    #     return df

    def getENIInfo(self,region_id,instance_list=None,page_size=500):
        client = AcsClient(region_id=region_id, credential=self.Credentials)
        request = DescribeNetworkInterfacesRequest()
        request.set_accept_format('json')
        request.set_MaxResults(page_size)
        if instance_list is not None:
            request.set_NetworkInterfaceIds(instance_list)
        response_json = json.loads(client.do_action_with_exception(request))
        df = pd.DataFrame(response_json['NetworkInterfaceSets']['NetworkInterfaceSet'])
        while(response_json.get('NextToken') is not None):
            request.set_NextToken(response_json['NextToken'])    
            response_json = json.loads(client.do_action_with_exception(request))
            df_clip = pd.DataFrame(response_json['NetworkInterfaceSets']['NetworkInterfaceSet'])
            df = pd.concat([df, df_clip],ignore_index=True)
        df['self_RegionId'] = region_id
        self.InsInfo = pd.concat([self.InsInfo,df],axis=0,ignore_index=True)
        return df
    
class RDS(AliyunInstance):
    def __init__(self,Credentials)-> None:
        super().__init__(Credentials)
        self.Namespace = "acs_rds_dashboard"
        self.ProductCategory = "rds"

    def getRDSInfo(self,region_id,instance_list=None,page_size=100):
        client = AcsClient(region_id=region_id, credential=self.Credentials)
        request = DescribeDBInstancesRequest()
        request.set_accept_format('json')
        request.set_MaxResults(page_size)
        if instance_list is not None:
            request.set_NetworkInterfaceIds(instance_list)
        response_json = json.loads(client.do_action_with_exception(request))
        df = pd.DataFrame(response_json['Items']['DBInstance'])
        while(response_json.get('NextToken') is not None or response_json.get('NextToken') !=''):
            pprint(response_json)
            pprint(response_json['NextToken'])
            request.set_NextToken(response_json['NextToken'])    
            response_json = json.loads(client.do_action_with_exception(request))
            df_clip = pd.DataFrame(response_json['Items']['DBInstance'])
            df = pd.concat([df, df_clip],ignore_index=True)
        df['self_RegionId'] = region_id
        self.InsInfo = pd.concat([self.InsInfo,df],axis=0,ignore_index=True)
        return df

class SLB(AliyunInstance):
    def __init__(self,Credentials)-> None:
        super().__init__(Credentials)
        self.Namespace = "acs_slb_dashboard"
        self.ProductCategory = "slb"
    
    # 未更新
    def getSLBInfo(self,region_id,inslist):
        client = AcsClient(region_id=region_id, credential=self.Credentials)
        request = DescribeLoadBalancersRequest()
        request.set_accept_format('json')
        request.set_LoadBalancerId(','.join(inslist))
        page_size = 10      # PageSize 最大值100,但是精准查询上限10个
        request.set_PageSize(page_size)
        request.set_PageNumber(1)
        df = pd.DataFrame()
        for idx in range(0, len(inslist),page_size):
            batch_inslist = inslist[idx:idx+page_size]
            request.set_LoadBalancerId(','.join(batch_inslist))
            response_json = json.loads(client.do_action_with_exception(request))
            df_clip = pd.DataFrame(response_json['LoadBalancers']['LoadBalancer'])
            df = pd.concat([df, df_clip],ignore_index=True)
        df['self_RegionId'] = region_id
        self.InsInfo = pd.concat([self.InsInfo,df],axis=0,ignore_index=True)
        return df

class NGW(AliyunInstance):
    def __init__(self,Credentials)-> None:
        super().__init__(Credentials)
        self.Namespace = "acs_nat_gateway"
        self.ProductCategory = "nat_gateway"

    def getNGWInfo(self,region_id,instance_list=None,page_size=50):
        client = AcsClient(region_id=region_id, credential=self.Credentials)
        request = DescribeNatGatewaysRequest()
        request.set_accept_format('json')
        request.set_PageSize(page_size)
        request.set_PageNumber(1)
        df = pd.DataFrame()
        if instance_list:
            page_size = 1
            for instance_str in instance_list:
                request.set_NatGatewayId(instance_str)
                response_json = json.loads(client.do_action_with_exception(request))
                df_clip = pd.DataFrame(response_json["NatGateways"]["NatGateway"])
                df = pd.concat([df, df_clip],ignore_index=True)
        else:
            request.set_PageNumber(1)
            response_json = json.loads(client.do_action_with_exception(request))
            df = pd.DataFrame(response_json["NatGateways"]["NatGateway"]) 
            page_total = response_json['TotalCount'] // page_size + 1
            for page_number in range(2,page_total+1):
                request.set_PageNumber(page_number)
                response_json = json.loads(client.do_action_with_exception(request))
                df_clip = pd.DataFrame(response_json["NatGateways"]["NatGateway"])
                df = pd.concat([df, df_clip],ignore_index=True)            
        df['self_RegionId'] = region_id
        self.InsInfo = pd.concat([self.InsInfo,df],axis=0,ignore_index=True)
        return df

class IPv6(AliyunInstance):
    def __init__(self,Credentials)-> None:
        super().__init__(Credentials)
        self.Namespace = "acs_ipv6_bandwidth"
        self.ProductCategory = "ipv6gateway"

    # 老版查询代码，未更新
    def getIPv6Info(self,region_id,page_size=50) -> pd.DataFrame:
        if page_size > 50:
            # print("传参错误")
            return None

        client = AcsClient(region_id=region_id, credential=self.Credentials) 
        request = DescribeIpv6AddressesRequest()
        request.set_accept_format('json')
        request.set_PageSize(1)
        request.set_PageNumber(1)
        response_json = json.loads(client.do_action_with_exception(request))
        page_total = response_json['TotalCount'] // page_size + 1
        df = pd.DataFrame()
        request.set_PageSize(page_size)
        for page_number in range(1,page_total+1):
            # time.sleep(sleep_time)
            request.set_PageNumber(page_number)
            response_json = json.loads(client.do_action_with_exception(request))
            df_temp = pd.DataFrame(response_json["Ipv6Addresses"]["Ipv6Address"]) 
            df = pd.concat([df, df_temp],axis=0,ignore_index=True)
        df['self_RegionId'] = region_id
        self.InsInfo = pd.concat([self.InsInfo,df],axis=0,ignore_index=True)
        return df
    
    def getIPv6Data(self,RegionList,MetricList,TimeDict=None,DisplayNameDict=None) -> None:
        """
        MetricList = ["Ipv6Address.RatePercentageOutToInternet","Ipv6Address.RatePercentageInFromInternet"]
        RegionList = ["cn-chengdu"]
        DisplayNameDict = {"Ipv6Address.RatePercentageOutToInternet":"OutToInternet","Ipv6Address.RatePercentageInFromInternet":"InFromInternet"}
        """
        ENI_ForMoreInfo = ENI(self.Credentials)
        ECS_ForMoreInfo = ECS(self.Credentials)
        print(f'Obtaining {self.InsType} Data in {RegionList}...')
        for region in RegionList:
            df = self.getIPv6Info(region)
            print(df)
            if not df.empty:
                # For getting more eni info
                eni_list = df[df["AssociatedInstanceType"]=="NetworkInterface"]["AssociatedInstanceId"].to_list()
                df_ENI = ENI_ForMoreInfo.getENIInfo(region,instance_list=eni_list)[["NetworkInterfaceId","InstanceId"]]
                df_ENI.rename(columns={"NetworkInterfaceId":"AssociatedInstanceId"},inplace=True)
                df = pd.merge(df,df_ENI,on="AssociatedInstanceId",how="left")
                # For getting more ecs info
                df.loc[df["AssociatedInstanceType"]=="EcsInstance","InstanceId"] = df.loc[df["AssociatedInstanceType"]=="EcsInstance", "AssociatedInstanceId"]
                df_ECS = ECS_ForMoreInfo.getECSInfo(region, df[df["InstanceId"].notna()]["InstanceId"].to_list())
                df = pd.merge(df,df_ECS[["InstanceId","InstanceName"]],on="InstanceId",how="left")
                # For getting more ipv6 info
                df = pd.concat([df, pd.json_normalize(df['Ipv6InternetBandwidth'])], axis=1)
                # Simplify the Info # 后续由self.drop
                df = df[["Ipv6AddressId","Ipv6Isp","NetworkType","InternetChargeType","Bandwidth","InstanceId","InstanceName"]]

                for metric in MetricList:
                    print(metric)
                    df_data = self.getMetricData(region,df["Ipv6AddressId"].to_list(),metric,TimeDict=TimeDict,DisplayMetricName=DisplayNameDict.get(metric))
                    print(df_data)
                    df_data.rename(columns={"instanceId":"Ipv6AddressId"},inplace=True)
                    df = pd.merge(df,df_data,on="Ipv6AddressId",how="left")
                self.InsData = pd.concat([self.InsData,df],axis=0,ignore_index=True)
                print(f"The {self.InsType} data of {region} is obtained!")
            else:
                print(f"No {self.InsType} in {region}!")
    
class Redis(AliyunInstance):
    def __init__(self,Credentials)-> None:
        super().__init__(Credentials)
        self.Namespace = "acs_kvstore"
        self.ProductCategory = ["kvstore_standard","kvstore_splitrw","kvstore_sharding"]

    def getReidsOverview(self,region_id) -> pd.DataFrame:
        client = AcsClient(region_id=region_id, credential=self.Credentials)
        request = DescribeInstancesOverviewRequest()
        request.set_accept_format('json')
        response_json = json.loads(client.do_action_with_exception(request))
        df = pd.DataFrame(response_json["Instances"]) 
        return df

    def getRedisInfo(self,region_id,instance_list=None,page_size=50) -> pd.DataFrame:
        client = AcsClient(region_id=region_id, credential=self.Credentials) 
        request = DescribeRedisInsRequest()
        request.set_accept_format('json')
        request.set_PageSize(page_size)
        request.set_PageNumber(1)
        if instance_list is None:
            response_json = json.loads(client.do_action_with_exception(request))
            df = pd.DataFrame(response_json["Instances"]["KVStoreInstance"]) 
            page_total = response_json['TotalCount'] // page_size + 1
            for page_number in range(2,page_total+1):
                request.set_PageNumber(page_number)
                response_json = json.loads(client.do_action_with_exception(request))
                df_clip = pd.DataFrame(response_json["Instances"]["KVStoreInstance"])
                df = pd.concat([df, df_clip],ignore_index=True)
        else:
            df = pd.DataFrame()
            page_size = 30
            request.set_PageSize(page_size)
            for idx in range(0, len(instance_list),page_size):
                batch_inslist_str = ",".join(instance_list[idx:idx+page_size])
                request.set_InstanceIds(batch_inslist_str)
                response_json = json.loads(client.do_action_with_exception(request))
                df_clip = pd.DataFrame(response_json["Instances"]["KVStoreInstance"])
                df = pd.concat([df, df_clip],ignore_index=True)
        df['self_RegionId'] = region_id
        self.InsInfo = pd.concat([self.InsInfo,df],axis=0,ignore_index=True)
        return df
        
    def getRedisData(self,RegionList,MetricList,TimeDict,instance_list=None)-> None:
        print(f'Obtaining {self.InsType} Data in {RegionList}...')
        for region in RegionList:
            if instance_list and len(RegionList) == 1:
                df = self.getRedisInfo(region,instance_list)
            else:
                df = self.getRedisInfo(region)
            # self.InsInfo[region] = self.getRedisInfo(region)
            # df format
            print(df)
            if not df.empty:
                cluster_list = df[df['ArchitectureType']=="cluster"]["InstanceId"].to_list()
                standard_list = df[df['ArchitectureType']=="standard"]["InstanceId"].to_list()
                rwsplit_list = df[df['ArchitectureType']=="rwsplit"]["InstanceId"].to_list()
                for metric_name in MetricList:
                    print(metric_name)
                    df_data = pd.concat(
                        [
                            self.getMetricData(region, cluster_list, "Sharding"+metric_name, TimeDict=TimeDict, DisplayMetricName=metric_name),
                            self.getMetricData(region, standard_list, "Standard"+metric_name, TimeDict=TimeDict, DisplayMetricName=metric_name),
                            self.getMetricData(region, rwsplit_list, "Splitrw"+metric_name, TimeDict=TimeDict, DisplayMetricName=metric_name)
                        ],
                        ignore_index=True
                    )
                    df = pd.merge(df, df_data.rename(columns={"instanceId":"InstanceId"}), on='InstanceId')
                self.InsData = pd.concat([self.InsData,df],ignore_index=True)
                print(f"The {self.InsType} data of {region} is obtained!")
            else:
                print(f"No {self.InsType} in {region}!")

# MongoDB
class DDS(AliyunInstance):
    def __init__(self,Credentials)-> None:
        super().__init__(Credentials)
        self.Namespace = "acs_mongodb"
        self.ProductCategory = "mongodb_replicaset"
        self.InsType = "replicate"

    def getInsInfo(self,region_id,instance_list=None,page_size=100):
        client = AcsClient(region_id=region_id, credential=self.Credentials)
        request = DescribeDBInstancesRequest()
        request.set_accept_format('json')
        request.set_PageSize(page_size)
        request.set_DBInstanceType(self.InsType)
        response_json = json.loads(client.do_action_with_exception(request))
        df_data = pd.DataFrame(response_json["DBInstances"]["DBInstance"]) 
        page_total = response_json['TotalCount'] // page_size + 1
        for page_number in range(2,page_total+1):
            request.set_PageNumber(page_number)
            response_json = json.loads(client.do_action_with_exception(request))
            df_data_tmp = pd.DataFrame(response_json["DBInstances"]["DBInstance"])
            df_data = pd.concat([df_data, df_data_tmp],ignore_index=True)            
        df_data['self_RegionId'] = region_id
        self.InsInfo = pd.concat([self.InsInfo,df_data],axis=0,ignore_index=True)
        return df_data

class EIP(AliyunInstance):
    def __init__(self,Credentials)-> None:
        super().__init__(Credentials)
        self.Namespace = "acs_vpc_eip"
        self.ProductCategory = "eip"

    def getEIPInfo(self,region_id,instance_list=None,ip_address_list=None,associate_instance_type=None,associate_instance_id=None,page_size=50) -> pd.DataFrame:
        client = AcsClient(region_id=region_id, credential=self.Credentials) 
        request = DescribeEipAddressesRequest()
        request.set_accept_format('json')
        request.set_PageSize(page_size)
        df = pd.DataFrame()
        # 先这样，后面考虑动态调用
        if associate_instance_type:
            request.set_AssociatedInstanceType(associate_instance_type)
        if instance_list:
            for idx in range(0, len(instance_list),page_size):
                batch_inslist_str = ",".join(instance_list[idx:idx+page_size])
                request.set_AllocationId(batch_inslist_str)
                response_json = json.loads(client.do_action_with_exception(request))
                df_clip = pd.DataFrame(response_json["EipAddresses"]["EipAddress"])
                df = pd.concat([df, df_clip],ignore_index=True)
        elif ip_address_list:
            for idx in range(0, len(ip_address_list),page_size):
                batch_ip_str = ",".join(ip_address_list[idx:idx+page_size])
                request.set_EipAddress(batch_ip_str)
                response_json = json.loads(client.do_action_with_exception(request))
                df_clip = pd.DataFrame(response_json["EipAddresses"]["EipAddress"])
                df = pd.concat([df, df_clip],ignore_index=True)
        # elif associate_instance_id:
        #     for idx in range(0, len(associate_instance_id),page_size):
        #         batch_inslist_str = ",".join(associate_instance_id[idx:idx+page_size])
        #         request.set_AssociatedInstanceId(batch_inslist_str)
        #         response_json = json.loads(client.do_action_with_exception(request))
        #         df_clip = pd.DataFrame(response_json["EipAddresses"]["EipAddress"])
        #         df = pd.concat([df, df_clip],ignore_index=True)
        else:
            request.set_PageNumber(1)
            response_json = json.loads(client.do_action_with_exception(request))
            df = pd.DataFrame(response_json["EipAddresses"]["EipAddress"]) 
            page_total = response_json['TotalCount'] // page_size + 1
            for page_number in range(2,page_total+1):
                request.set_PageNumber(page_number)
                response_json = json.loads(client.do_action_with_exception(request))
                df_clip = pd.DataFrame(response_json["EipAddresses"]["EipAddress"])
                df = pd.concat([df, df_clip],ignore_index=True)            
        df['self_RegionId'] = region_id
        self.InsInfo = pd.concat([self.InsInfo,df],axis=0,ignore_index=True)
        return df
    
    # 没写完，尽量再保留一列AssociatedInstanceId
    def getBindedInsInfo(self,merge=True):
        df_ins_all = pd.DataFrame()
        if self.InsInfo.empty:
            print("No EIP Info!Pls get info frist!")
        else:
            for region in self.InsInfo['self_RegionId'].unique():
                df_region_InsInfo = self.InsInfo[self.InsInfo['self_RegionId']==region]
                df_ins = pd.DataFrame({"InstanceId":[], "InstanceName":[]})
                instance_list = df_region_InsInfo[df_region_InsInfo["InstanceType"] == "NetworkInterface"]["InstanceId"].to_list()
                if len(instance_list) > 0:
                    MoreInfo = ENI(self.Credentials)
                    df_ENI = MoreInfo.getENIInfo(region,instance_list)[['NetworkInterfaceId','InstanceId']].dropna(subset=['InstanceId'])
                    if len(df_ENI) > 0:
                        MoreInfo = ECS(self.Credentials)
                        df_ENI_ECS = MoreInfo.getECSInfo(region,df_ENI['InstanceId'].drop_duplicates().to_list())[['InstanceId','InstanceName']]
                        df_ENI_ECS = pd.merge(df_ENI,df_ENI_ECS,how="left",on='InstanceId').drop(['InstanceId'],axis=1)
                        df_ENI_ECS.rename(columns={'NetworkInterfaceId':'InstanceId'},inplace=True)
                        df_ins = pd.concat([df_ins,df_ENI_ECS],axis=0,ignore_index=True)
                instance_list = df_region_InsInfo[df_region_InsInfo["InstanceType"] == "EcsInstance"]["InstanceId"].to_list()
                if len(instance_list) > 0:
                    MoreInfo = ECS(self.Credentials)
                    df_ECS = MoreInfo.getECSInfo(region,instance_list)[['InstanceId','InstanceName']]
                    df_ins = pd.concat([df_ins,df_ECS],axis=0,ignore_index=True)
                instance_list = df_region_InsInfo[df_region_InsInfo["InstanceType"] == "SlbInstance"]["InstanceId"].drop_duplicates().to_list()
                if len(instance_list) > 0:
                    MoreInfo = SLB(self.Credentials)
                    df_SLB = MoreInfo.getSLBInfo(region,instance_list)[['LoadBalancerId','LoadBalancerName']]
                    df_SLB.rename(columns={'LoadBalancerId':'InstanceId','LoadBalancerName':'InstanceName'},inplace=True)
                    df_ins = pd.concat([df_ins,df_SLB],axis=0,ignore_index=True)
                instance_list = df_region_InsInfo[df_region_InsInfo["InstanceType"] == "Nat"]["InstanceId"].drop_duplicates().to_list()
                if len(instance_list) > 0:
                    MoreInfo = NGW(self.Credentials)
                    df_NGW = MoreInfo.getNGWInfo(region,instance_list)[['NatGatewayId','Name']]
                    df_NGW.rename(columns={'NatGatewayId':'InstanceId','Name':'InstanceName'},inplace=True)
                    df_ins = pd.concat([df_ins,df_NGW],axis=0,ignore_index=True)
                df_ins_all = pd.concat([df_ins_all,df_ins],axis=0,ignore_index=True)
            if merge:
                self.InsInfo = pd.merge(self.InsInfo,df_ins_all,how="left",on='InstanceId')
        return df_ins_all

    # def getStatementData(YoY=True,)

    def getInsData(self,MetricList,TimeDict,RegionList=None,DisplayNameDict=None) -> None:
        if self.InsInfo.empty:
            # if RegionList is None:
                # RegionList = describeavialableregions().regions
            print(f'Obtaining {self.InsType} Info in {RegionList}...')
            for RegionId in RegionList:
                self.getEIPInfo(RegionId)
            self.getBindedInsInfo()
        else:
            print("检索到InsInfo已有数据，将直接查询InsInfo内各实例的指标数据，RegionList参数无效！")
        df = self.InsInfo.copy()
        RegionList = df['RegionId'].unique().tolist()
        # for region in RegionList:
        # 不再循环设置regionid，所有云监控数据从cn-hangzhou获取
        for metric in MetricList:
            print(f'Obtaining {metric} Data of {self.InsType} ...')
            df_data = self.getMetricData("cn-hangzhou",df["BandwidthPackageId"].to_list(),metric,TimeDict=TimeDict)
            df_data.rename(columns={"instanceId":"BandwidthPackageId"},inplace=True)
            df = pd.merge(df,df_data,on="BandwidthPackageId",how="left")
        self.InsData = pd.concat([self.InsData,df],axis=0,ignore_index=True)
        print(f"The {self.InsType} data is obtained!")
        

    # 未写完
    # def getEIPBwRank(self,instance_list,metric_name,query_datetime,TimeDict,region_id="cn-hangzhou"):
    #     df_temp = self.getMetricData(region_id,instance_list=instance_list,metric_name=metric_name,TimeDict=TimeDict,Period='60',DisplayMetricName="当前速率(bps)",KeepTimestamp=True)
    #     df_offset_temp = self.getMetricData(region_id,instance_list=instance_list,metric_name=metric_name,TimeDict=getTimeDict(end_datetime=TimeDict['end_datetime']-datetime.timedelta(days=1)),Period='60',DisplayMetricName="昨日速率(bps)",KeepTimestamp=True)
    #     df_data = pd.merge(df_temp,df_offset_temp,on=['instanceId'],how='left')
    #     df_data.drop(['timestamp_y','当前速率(bps)_95','昨日速率(bps)_95'],inplace=True,axis=1)
    #     df_data['timestamp_x'] = pd.to_datetime(df_data['timestamp_x'],unit="ms",origin='1970-01-01 08:00:00').dt.strftime('%H:%M')
    #     df_data['当前速率(bps)'] = df_data['当前速率(bps)'].div(1024**2).round(2)
    #     df_data["昨日速率(bps)"] = df_data["昨日速率(bps)"].div(1024**2).round(2)
    #     df_data.rename(columns={'instanceId':'AllocationId','timestamp_x':'时间','当前速率(bps)':'当前速率(Mbps)','昨日速率(bps)':'昨日速率(Mbps)'},inplace=True)
    #     df_data['差值'] = df_data['当前速率(Mbps)'] - df_data['昨日速率(Mbps)']
    #     df_data.sort_values(by=['当前速率(Mbps)'],ascending=False,inplace=True,)
    #     return df_data
                
class CBWP(AliyunInstance):
    def __init__(self,Credentials)-> None:
        super().__init__(Credentials)
        self.Namespace = "acs_bandwidth_package"
        self.ProductCategory = "sharebandwidthpackages"
        self.EIPsFromAllCBWP = None

    def getCBWPInfo(self,region_id,instance_list=None,page_size=50) -> pd.DataFrame:
        client = AcsClient(region_id=region_id, credential=self.Credentials)
        request = DescribeCommonBandwidthPackagesRequest()
        request.set_accept_format('json')
        request.set_PageSize(page_size)
        request.set_PageNumber(1)
        if instance_list is None:
            response_json = json.loads(client.do_action_with_exception(request))
            df = pd.DataFrame(response_json['CommonBandwidthPackages']['CommonBandwidthPackage'])
            page_total = response_json['TotalCount'] // page_size + 1
            for page_number in range(2,page_total+1):
                request.set_PageNumber(page_number)
                response_json = json.loads(client.do_action_with_exception(request))
                df_clip = pd.DataFrame(response_json['CommonBandwidthPackages']['CommonBandwidthPackage'])
                df = pd.concat([df, df_clip],ignore_index=True)
        else:
            # Only one cbwpid can be queried, not real batch query :D
            df = pd.DataFrame()
            page_size = 1
            request.set_PageSize(page_size)
            for cbwpid in instance_list:
                request.set_BandwidthPackageId(cbwpid)
                response_json = json.loads(client.do_action_with_exception(request))
                df_clip = pd.DataFrame(response_json['CommonBandwidthPackages']['CommonBandwidthPackage'])
                df = pd.concat([df, df_clip],ignore_index=True)
        df['self_RegionId'] = region_id
        self.InsInfo = pd.concat([self.InsInfo,df],axis=0,ignore_index=True)
        return df
    
    # 可能需要优化
    def extractEIPFromInsInfo(self):
        df_EIP = pd.DataFrame()
        if self.InsInfo.empty:
            print(f'No {self.InsType} Data Found...')
        else:
            df = self.InsInfo
            df['PublicIpAddresses'] = df['PublicIpAddresses'].apply(lambda x: x['PublicIpAddresse'])
            for index,row in df.iterrows():
                df_temp = pd.DataFrame(row['PublicIpAddresses'])
                df_temp['BandwidthPackageId'] = row['BandwidthPackageId']
                df_temp['BandwidthPackageName'] = row['Name']
                df_temp['RegionId'] = row['RegionId']
                df_EIP = pd.concat([df_EIP, df_temp],ignore_index=True)
            # print(f'Extracted EIP from {self.InsType} Data...')
        self.EIPsFromAllCBWP = df_EIP
        return df_EIP
    
    def extractEIPFromSingleCBWP(self,PublicIpAddresses)->pd.DataFrame:
        df_data = pd.DataFrame(PublicIpAddresses['PublicIpAddresse'])
        return df_data
        
    def getEIPBandwidthRank(self,TimeDict,flow_direction='out',display_size=None):
        if flow_direction == "out":
            metric_name = "net_tx.rate"
        elif flow_direction == "in":
            metric_name = "net_rx.rate"
        else:
            raise ValueError("flow_direction must be 'out' or 'in'")
        EIPForBwRank = EIP(self.Credentials)
        df_data = pd.DataFrame()
        for index,row in self.InsInfo.iterrows():
            df_cbwp_eip = self.extractEIPFromSingleCBWP(row['PublicIpAddresses']) 
            instance_list = df_cbwp_eip['AllocationId'].tolist()
            df_temp = EIPForBwRank.getMetricData(instance_list=instance_list,metric_name=metric_name,TimeDict=TimeDict,period='60',DisplayMetricName="查时速率(bps)")
            df_offset_temp = EIPForBwRank.getMetricData(instance_list=instance_list,metric_name=metric_name,TimeDict=getTimeDict(end_offset_days=1,end_datetime=TimeDict['end_datetime']),period='60',DisplayMetricName="同比速率(bps)")
            df_data = pd.concat([df_data,pd.merge(df_temp,df_offset_temp,on=['instanceId'],how='left')],ignore_index=True)
            EIPForBwRank.getEIPInfo(row['RegionId'],instance_list=instance_list)
        # formatting
        df_data.rename(columns={'instanceId':'AllocationId'},inplace=True)
        EIPForBwRank.InsInfo = pd.merge(df_data,EIPForBwRank.InsInfo,on=['AllocationId'],how='left')
        EIPForBwRank.InsInfo['当前(Mbps)'] = EIPForBwRank.InsInfo['查时速率(bps)_max'].div(1000**2).round(2)
        EIPForBwRank.InsInfo["同比(Mbps)"] = EIPForBwRank.InsInfo["同比速率(bps)_max"].div(1000**2).round(2)
        EIPForBwRank.InsInfo['差值(Mbps)'] = EIPForBwRank.InsInfo['当前(Mbps)'] - EIPForBwRank.InsInfo['同比(Mbps)']
        EIPForBwRank.InsInfo['BandwidthPackageBandwidth'] = EIPForBwRank.InsInfo['BandwidthPackageBandwidth'].astype(int)
        EIPForBwRank.InsInfo['占比(%)'] = ((EIPForBwRank.InsInfo['当前(Mbps)'] / EIPForBwRank.InsInfo['BandwidthPackageBandwidth'])*100).round(2)
        EIPForBwRank.InsInfo = EIPForBwRank.InsInfo.sort_values(by=['当前(Mbps)'],ascending=False).reset_index(drop=True)
        if display_size:
            EIPForBwRank.InsInfo = EIPForBwRank.InsInfo.head(display_size)
        EIPForBwRank.getBindedInsInfo()
        return EIPForBwRank.InsInfo
    
    # v1.0.6 ，如果self.InsInfo有数据，则直接使用，没有则获取各地域下的所有资源，正式更名为getInsData
    def getInsData(self,MetricList,TimeDict,RegionList=None,DisplayNameDict=None) -> None:
        if self.InsInfo.empty:
            # if RegionList is None:
                # RegionList = describeavialableregions().regions
            print(f'Obtaining {self.InsType} Info in {RegionList}...')
            for RegionId in RegionList:
                self.getCBWPInfo(RegionId)
        else:
            print("检索到InsInfo已有数据，将直接查询InsInfo内各实例的指标数据，RegionList参数无效！")
        df = self.InsInfo.copy()
        RegionList = df['RegionId'].unique().tolist()
        # for region in RegionList:
        # 不再循环设置regionid，所有云监控数据从cn-hangzhou获取
        for metric in MetricList:
            print(f'Obtaining {metric} Data of {self.InsType} ...')
            df_data = self.getMetricData("cn-hangzhou",df["BandwidthPackageId"].to_list(),metric,TimeDict=TimeDict)
            df_data.rename(columns={"instanceId":"BandwidthPackageId"},inplace=True)
            df = pd.merge(df,df_data,on="BandwidthPackageId",how="left")
        self.InsData = pd.concat([self.InsData,df],axis=0,ignore_index=True)
        print(f"The {self.InsType} data is obtained!")

class AntiDDoS():
    def __init__(self,Credentials)-> None:
        self.InsType = self.__class__.__name__
        self.Credentials:dict = Credentials
        self.InsInfo = pd.DataFrame({"InternetIp":[],"InstanceId":[],"InstanceName":[],"InstanceType":[],"Region":[]})
    
    def getIPRegion(self,ip:str=None)-> None:
        if ip:
            client = AcsClient(region_id='cn-hangzhou', credential=self.Credentials)
            request = CommonRequest()
            request.set_accept_format('json')
            request.set_domain('antiddos.aliyuncs.com')
            request.set_method('POST')
            request.set_protocol_type('https') # https | http
            request.set_version('2017-05-18')
            request.set_action_name('DescribeIpLocationService')
            request.add_query_param('InternetIp', ip)
            response_json = json.loads(client.do_action(request))
            response = response_json.get('Instance')
            if response:
                df = pd.DataFrame([response])
                self.InsInfo = pd.concat([self.InsInfo,df],axis=0,ignore_index=True)
            else:
                df = pd.DataFrame()
        else:
            df = pd.DataFrame()
        return df
            
    def getIPInfo(self):
        df = self.InsInfo[self.InsInfo['InstanceType']=="eip"]
        if not df.empty:
            IP_MoreInfo = EIP(self.Credentials)
            for region in self.InsInfo['Region'].unique():
                instance_list = df[df['Region']==region]["InstanceId"].to_list()
                IP_MoreInfo.getEIPInfo(region_id=region,instance_list=instance_list)
            IP_MoreInfo.getBindedInsInfo()
            ipinfo = IP_MoreInfo.InsInfo[['AllocationId','InstanceId','InstanceName','BandwidthPackageId']].rename(columns={'InstanceId':'AssociatedInstanceId','AllocationId':'InstanceId'})
            self.InsInfo = self.InsInfo.merge(ipinfo, on='InstanceId', how='left', suffixes=('', '_B'))
            self.InsInfo['InstanceName'] = self.InsInfo['InstanceName'].fillna(self.InsInfo['InstanceName_B'])
            self.InsInfo.drop(columns=['InstanceName_B'], inplace=True)

class OSS(AliyunInstance):
    def __init__(self, Credentials):
        super().__init__(Credentials)
        self.auth = oss2.ProviderAuth(StaticCredentialsProvider(self.Credentials.access_key_id, self.Credentials.access_key_secret))

    def getOSSInfo(self,max_retries=3):
        service = oss2.Service(self.auth, 'https://oss-cn-hangzhou.aliyuncs.com')
        oss_list = {"Name":[],"Region":[],"StorageClass":[],"CreationDate":[],'IntranetEndpoint':[],'ExtranetEndpoint':[],"OwnerId":[],"ACLGrant":[],"DataRedundancyType":[],"AccessMonitor":[],"StorageSizeInBytes":[],"Tag":[]}
        retries = 0
        for object in oss2.BucketIterator(service):
            # oss_list['Name'].append(object.name)
            while retries < max_retries:
                try:
                    print(object.name,end="...")
                    bucket = oss2.Bucket(self.auth, 'https://oss-cn-hangzhou.aliyuncs.com', object.name)
                    oss_region = bucket.get_bucket_location().location
                    bucket_info = bucket.get_bucket_info()
                    bucket = oss2.Bucket(self.auth,bucket_info.extranet_endpoint,object.name)
                    bucket_stat = bucket.get_bucket_stat()
                    bucket_tagging = bucket.get_bucket_tagging()
                    # 先请求到所有数据再插入，简单保证原子性
                    oss_list['Region'].append(oss_region)
                    oss_list['Name'].append(bucket_info.name)
                    oss_list['StorageClass'].append(bucket_info.storage_class)
                    oss_list['CreationDate'].append(bucket_info.creation_date)
                    oss_list['IntranetEndpoint'].append(bucket_info.intranet_endpoint)
                    oss_list['ExtranetEndpoint'].append(bucket_info.extranet_endpoint)
                    oss_list['OwnerId'].append(bucket_info.owner.id)
                    oss_list['ACLGrant'].append(bucket_info.acl.grant)
                    oss_list['DataRedundancyType'].append(bucket_info.data_redundancy_type)
                    oss_list['AccessMonitor'].append(bucket_info.access_monitor)
                    # oss_list.setdefault('StorageSizeInBytes', []).append(bucket_stat.storage_size_in_bytes)
                    oss_list['StorageSizeInBytes'].append(bucket_stat.storage_size_in_bytes)
                    oss_list['Tag'].append(bucket_tagging.tag_set.tagging_rule)
                    print("获取成功")
                    break
                except requests.exceptions.ConnectTimeout:
                    retries += 1
                    time.sleep(1)
                    if retries == max_retries:
                        print(f"\nFailed to get info for bucket {object.name} after {max_retries} attempts.")
                    else:
                        print(f"Retrying({retries})...",end="")
                # OSS获取非国内数据时因线路问题可能导致超时，非代码异常，当前方案加入单线程重试，后续也可考虑跳过所有超时后，再返回重试将空缺部分回填

class CenBWP(AliyunInstance):
    def __init__(self,Credentials)-> None:
        super().__init__(Credentials)
        self.Namespace = "acs_cen"
        self.ProductCategory = "cen_area"
        self.Dimensions = 'bandwidthPackageId'

class CenRegion(AliyunInstance):
    def __init__(self,Credentials)-> None:
        super().__init__(Credentials)
        self.Namespace = "acs_cen"
        self.ProductCategory = "cen_region"
        self.Dimensions = 'cenId'

class DDoSDip(AliyunInstance):
    def __init__(self,Credentials)-> None:
        super().__init__(Credentials)
        self.Namespace = "acs_ddosdip"
        self.ProductCategory = "ddosdip"
        
