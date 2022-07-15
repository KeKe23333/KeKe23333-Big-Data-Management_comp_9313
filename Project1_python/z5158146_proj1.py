import math
from collections import defaultdict, OrderedDict, Counter
from mrjob.job import MRJob
from mrjob.compat import jobconf_from_env
import re
import sys


class project1 (MRJob):
    total_year = jobconf_from_env('myjob.settings.years')
    # reduces_num = jobconf_from_env('myjob.settings.reduces')

    numDoc = jobconf_from_env('myjob.settings.docnumber')  # str



    def mapper_init(self):
        self.mapdict = defaultdict(lambda: defaultdict(int))

    def mapper(self, _, line):
        pat = r'\s+|,'
        doc = re.split(pat, line)
        if len(doc) <= 1: return
        year = doc[0][0:4]
        for i in range(len(doc) - 1):
                x = i + 1
                self.mapdict[doc[x]][year] += 1
                # yield x, year
    def mapper_final(self):
        for k, v in self.mapdict.items():
            yield (k, v)
    # def reducer(self, key, values):
    #     for v in values:
    #         yield key, v


    def reducer(self, key, values, total_year=None):

        tmp_dict = {}
        res = {}
        for sub_dict in values:
            inter_dict_1 = Counter(sub_dict)
            inter_dict_2 = Counter(tmp_dict)
            tmp_dict = dict(inter_dict_1 + inter_dict_2)

        tmp_dict = OrderedDict(sorted(tmp_dict.items()))
        res[key] = tmp_dict
        # for k, v in res.items():
        #     for i in v:
        #         yield  k, i
        #
        # res :  "adelaide"	{"2019":2,"2020":1,"2021":1}
        for k, v in res.items():
            df = len(v)
            temRes = []

            for o in v:
                    weight = res[k][o] * math.log10(int(self.total_year)/df)
                    year = o
                    temRes.append(year + "," + str(weight))


            temRes = str(temRes).replace('[', '')
            temRes = str(temRes).replace(']', '')
            temRes = str(temRes).replace("'", '')
            temRes = str(temRes).replace(', ', ';')
            if (k!= ""):
                yield k, temRes
                # yield k, year + "," + str(res[k][o]) + "," + str(df)




        # tmp = defaultdict(lambda: defaultdict(int))
        # tmpdict = defaultdict(lambda: defaultdict(int))
        # count = sum(values)
        # term, year = key.split(".")
        # self.tmpdict[term][year] = count


    SORT_VALUES = True
    JOBCONF = {
        'mapreduce.map.output.key.field.separator': '.',
        'mapreduce.job.reduces': 2,
        'partitioner':'org.apache.hadoop.mapred.lib.KeyFieldBasedPartitioner',
        'mapreduce.partition.keypartitioner.options': '-k1,1',
        'mapreduce.job.output.key.comparator.calss': 'org.apache.hadoop.mapreduce.lib.partition.KeyFieldBasedComparator',
        'mapreduce.partition.keycomparator.options': '-k1,1 -k2,2n'
    }




if __name__ =='__main__':
    # sys.stdout = open('result.txt', 'w+')
    project1.run()
    # sys.stdout.close()

# #####################################################################
# 用special key 的方法
# #HashMap 来储存每个 term-year 对出现的次数， 相当于一个 InMapperCombiner
# termyearPairDict = defaultdict(int)
# #使用HashSet 来储存每个Doc 中的Term 便于 emit term-special Key, 1. 然后在reducer 进行累加计算
# termSet = set()
# for i in range(1, len(doc)):
#     currentTerm = doc[i].lower()
# #解析输入
# tempdoc = line.replace(',', ' ')
# doc = tempdoc.split(" ")
#
# if len(doc) <= 1:return
#
#
#
# #第一个字符串为 year
# year = doc[0][0:4]
# # year = doc[0]
#
# #从第二个单词开始遍历
#     if len(currentTerm) == 0: continue
#
# #判断，将数据写入
#     if(currentTerm not in termSet):
#         termSet.add(currentTerm)
#
#     #判断将数据放入 termyearPairDict
#     termyearPairDict[str(currentTerm) + "." + str(year)] += 1
#
# # for i in termSet:
# #     yield str(i) + "." + year, 1
#
# for k, v in termyearPairDict.items():
#     yield k, v
# #输出的 格式 k =  woman.2019    v =2
# #          k =   adelaide.2019 v = 1