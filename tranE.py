#!/usr/bin/python
# -*- coding: UTF-8 -*-
from random import uniform, sample
from numpy import *
from copy import deepcopy
import MySQLdb

class TransE:
    def __init__(self, margin = 1, learingRate = 0.00001, dim = 10, L1 = True):
        self.margin = margin
        self.learingRate = learingRate
        self.dim = dim#向量维度
        self.loss = 0
        self.L1 = L1

    #训练函数
    def transE(self, cI = 20):
        if self.connectDB() == -1:
            return 
        print("Succeed to connect database\n")
        if self.initialize() == -1:
            return 
        print("Vectors initialized\n")
        print("Training starts\n")
        for cycleIndex in range(cI):#训练20次
            Sbatch = self.getSample(3)#获取150个三元组
            Tbatch = []#元组对（原三元组，打碎的三元组）的列表 ：[((h,r,t),(h',r,t'))]
            for sbatch in Sbatch:
                mixTriplet = (sbatch, self.getCorruptedTriplet(sbatch))
                if(mixTriplet not in Tbatch):
                    Tbatch.append(mixTriplet)
            self.update(Tbatch)#训练一次
            if cycleIndex % 100 == 0:
                print("The %dth loop"%cycleIndex)
                print("loss:%f"%self.loss)
                self.writeVectorToDB()
                self.loss = 0
        self.closeDB()

    #连接数据库
    def connectDB(self):
        try:
            self.dbc = MySQLdb.connect(host="localhost",user="root",passwd="",db="music3")
        except:
            print("Fail to connect database")
            return -1

    #初始化向量以及知识库,为训练做准备
    def initialize(self):
        
        cursor = self.dbc.cursor()

        #从user表取出数据，用于向量初始化,得到字典
        userVectorList = {}#key为id value为向量array
        sql = "SELECT * FROM user"
        try:
            cursor.execute(sql)
            allUsers = cursor.fetchall()
        except:
            print("Error: unable to fetch user data")
            cursor.close()
            return -1

        #用户向量初始化
        for line in allUsers:
            try:
                sql = "SELECT * FROM user_vector WHERE id = %d" % line[0]
                cursor.execute(sql)
                oneUserVector = cursor.fetchone()
            except:
                print("Error: unable to fetch user vector data")
                cursor.close()
                return -1
            if oneUserVector == None:
                n = 0
                userVector = []
                while n < self.dim:
                    ram = init(self.dim)#初始化的范围
                    userVector.append(ram)
                    n += 1
                userVector = norm(userVector)#归一化
                userVectorList[line[0]] = userVector    
                try:
                    sql = "INSERT INTO user_vector VALUES(%d, '%s')"%(line[0], str(userVector.tolist()))
                    cursor.execute(sql)
                    self.dbc.commit()
                except:
                    self.dbc.rollback()
                    printf("Error:write initialized user vector to database")
                    cursor.close()
                    return -1
            else:
                userVectorList[line[0]] = self.loadData(oneUserVector[1])
            

        print("User vectors initialized,the num is %d"%len(userVectorList))

        #关系向量初始化
        relationVectorList = {}
        for line in allUsers:
            try:
                sql = "SELECT * FROM relation_vector WHERE id = %d" % line[0]
                cursor.execute(sql)
                oneRelationVector = cursor.fetchone()
            except:
                print("Error: unable to fetch relation vector data")
                cursor.close()
                return -1
            if oneRelationVector == None:
                n = 0
                relationVector = []
                while n < self.dim:
                    ram = init(self.dim)#初始化的范围
                    relationVector.append(ram)
                    n += 1
                relationVector = norm(relationVector)#归一化
                relationVectorList[line[0]] = relationVector
                try:
                    sql = "INSERT INTO relation_vector VALUES(%d, '%s')"%(line[0], str(relationVector.tolist()))
                    cursor.execute(sql)
                    self.dbc.commit()
                except:
                    self.dbc.rollback()
                    printf("Error:write initialized relation vector to database")
                    cursor.close()
                    return -1   
            else :
                relationVectorList[line[0]] = self.loadData(oneRelationVector[1])

        

        print("Relation vectors initialized,the num is %d"%len(relationVectorList))

        #从song表取出数据，用于向量初始化,得到字典
        songVectorList = {}
        sql = "SELECT * FROM song"
        try:
            cursor.execute(sql)
            allSongs = cursor.fetchall()
        except:
            print("Error: unable to fetch song data")
            cursor.close()
            return -1

        #音乐向量初始化
        for line in allSongs:
            try:
                sql = "SELECT * FROM song_vector WHERE id = %d" % line[0]
                cursor.execute(sql)
                oneSongVector = cursor.fetchone()
                if oneSongVector == None:
                    n = 0
                    songVector = []
                    while n < self.dim:
                        ram = init(self.dim)#初始化的范围
                        songVector.append(ram)
                        n += 1
                    songVector = norm(songVector)#归一化
                    songVectorList[line[0]] = songVector
                    try:
                        sql = "INSERT INTO song_vector VALUES(%d, '%s')"%(line[0], str(songVector.tolist()))
                        cursor.execute(sql)
                        self.dbc.commit()
                    except:
                        self.dbc.rollback()
                        print("Error:write initialized song vector to database")
                        cursor.close()
                        return -1   
                else :
                    songVectorList[line[0]] = self.loadData(oneSongVector[1])
            except:
                print("Error: unable to fetch song vector data")
                cursor.close()
                return -1

        print("Song vectors initialized,the num is %d"%len(songVectorList))

        self.songList = songVectorList  #赋值到成员变量
        self.userList = userVectorList
        self.relationList = relationVectorList

        #从songlike表中取出数据，用于向量初始化
        sql = "SELECT * FROM songlike"
        try:
            cursor.execute(sql)
            kbRecords = cursor.fetchall()#kb表示知识库(knowledge base)
        except:
            print("Error: unable to fetch songlike data")
            cursor.close()
            return -1
        #知识库List初始化,知识库为List,每个元素为tuple
        i = 0 
        self.tripleList = []
        for line in kbRecords:
            triple = []
            triple.append(line[1]);
            triple.append(line[1]);
            triple.append(line[2]);
            self.tripleList.append(tuple(triple))
        cursor.close()    
        print("Knowledge base initialized,the num is %d"%len(self.tripleList))


    #随机抽取size个三元组
    def getSample(self, size):
        return sample(self.tripleList, size)

    def getCorruptedTriplet(self, triplet):
        '''
        training triplets with either the head or tail replaced by a random entity (but not both at the same time)
        :param triplet:
        :return corruptedTriplet:
        '''
        i = uniform(-1, 1)
        if i < 0:#小于0，打坏三元组的第一项
            while True:
                entityTemp = sample(self.userList.keys(), 1)[0]
                if entityTemp != triplet[0]:
                    break
            corruptedTriplet = (entityTemp, triplet[1], triplet[2])
        else:#大于等于0，打坏三元组的第二项
            while True:
                entityTemp = sample(self.songList.keys(), 1)[0]
                if entityTemp != triplet[1]:
                    break
            corruptedTriplet = (triplet[0], triplet[1],entityTemp)
        return corruptedTriplet

    #更新向量
    def update(self, Tbatch):
        copyUserList = deepcopy(self.userList)
        copyRelationList = deepcopy(self.relationList)
        copySongList = deepcopy(self.songList)

        for mixTriplet in Tbatch:
            #取正样本head,relation,tail
            posHeadVector = copyUserList[mixTriplet[0][0]]#tripletWithCorruptedTriplet是原三元组和打碎的三元组的元组tuple
            posRelationVector = copyRelationList[mixTriplet[0][1]]
            posTailVector = copySongList[mixTriplet[0][2]]
            #取负样本head
            negHeadVector = copyUserList[mixTriplet[1][0]]
            negTailVector = copySongList[mixTriplet[1][2]]
            #取拷贝之前的样本,防止拷贝后的数据更新
            oriPosHeadVector = self.userList[mixTriplet[0][0]]#tripletWithCorruptedTriplet是原三元组和打碎的三元组的元组tuple
            oriPosRelationVector = self.relationList[mixTriplet[0][1]]
            oriPosTailVector = self.songList[mixTriplet[0][2]]
            oriNegHeadVector = self.userList[mixTriplet[1][0]]
            oriNegTailVector = self.songList[mixTriplet[1][2]]
            
            if self.L1:
                distTriplet = distanceL1(oriPosHeadVector, oriPosRelationVector, oriPosTailVector)
                distCorruptedTriplet = distanceL1(oriNegHeadVector, oriPosRelationVector,  oriNegTailVector)
            else:
                distTriplet = distanceL2(oriPosHeadVector, oriPosRelationVector, oriPosTailVector)
                distCorruptedTriplet = distanceL2(oriNegHeadVector, oriPosRelationVector ,  oriNegTailVector)
            eg = self.margin + distTriplet - distCorruptedTriplet
            if eg > 0: #[function]+ 是一个取正值的函数
                self.loss += eg
                if self.L1:
                    tempPositive = 2 * self.learingRate * (oriPosTailVector - oriPosHeadVector - oriPosRelationVector)
                    tempNegtative = 2 * self.learingRate * (oriNegTailVector - oriNegHeadVector - oriPosRelationVector)
                    tempPositiveL1 = []
                    tempNegtativeL1 = []
                    for i in range(self.dim):#不知道有没有pythonic的写法（比如列表推倒或者numpy的函数）？
                        if tempPositive[i] >= 0:
                            tempPositiveL1.append(1)
                        else:
                            tempPositiveL1.append(-1)
                        if tempNegtative[i] >= 0:
                            tempNegtativeL1.append(1)
                        else:
                            tempNegtativeL1.append(-1)
                    tempPositive = array(tempPositiveL1)  
                    tempNegtative = array(tempNegtativeL1)

                else:
                    tempPositive = 2 * self.learingRate * (oriPosTailVector - oriPosHeadVector - oriPosRelationVector)
                    tempNegtative = 2 * self.learingRate * (oriNegTailVector - oriNegHeadVector - oriPosRelationVector)
    
                posHeadVector = posHeadVector + tempPositive
                posTailVector = posTailVector - tempPositive
                posRelationVector = posRelationVector + tempPositive - tempNegtative
                negHeadVector = negHeadVector - tempNegtative
                negTailVector = negTailVector + tempNegtative

                #只归一化这几个刚更新的向量，而不是按原论文那些一口气全更新了
                copyUserList[mixTriplet[0][0]] = norm(posHeadVector)
                copyRelationList[mixTriplet[0][1]] = norm(posRelationVector)
                copySongList[mixTriplet[0][2]] = norm(posTailVector)
                copyUserList[mixTriplet[1][0]] = norm(negHeadVector)
                copySongList[mixTriplet[1][2]] = norm(negTailVector)
                
        self.userList = copyUserList
        self.relationList = copyRelationList
        self.songList = copySongList

    def writeVectorToDB(self):
        cursor = self.dbc.cursor()
        try:
            for userID in self.userList.keys():

                sql = "UPDATE user_vector SET uservector = '%s' WHERE id = %d" % (str(self.userList[userID].tolist()),userID)
                cursor.execute(sql)

            for songID in self.songList.keys():

                sql = "UPDATE song_vector SET songvector = '%s' WHERE id = %d" % (str(self.songList[songID].tolist()),songID)
                cursor.execute(sql)

            for relationID in self.relationList.keys():
                sql = "UPDATE relation_vector SET relationvector = '%s' WHERE id = %d" % (str(self.relationList[relationID].tolist()),relationID)
                cursor.execute(sql)
            self.dbc.commit()
        except:
            self.dbc.rollback()
            print("write relation vectors to database error!")
            cursor.close()
            return -1

        cursor.close()

    def closeDB(self):
        self.dbc.close()

    def loadData(self,str):
        vecList = [float(s) for s in str[1:-1].split(", ")]
        return array(vecList)

def init(dim):
    return uniform(-6/(dim**0.5), 6/(dim**0.5))

def distanceL1(h, r ,t):
    s = h + r - t
    sum = fabs(s).sum()#fabs()浮点数取绝对值
    return sum

def distanceL2(h, r, t):
    s = h + r - t
    sum = (s*s).sum()
    return sum
 
def norm(list):
    '''
    归一化
    :param 向量
    :return: 向量的平方和的开方后的向量
    '''
    var = linalg.norm(list)
    i = 0
    while i < len(list):
        list[i] = list[i]/var
        i += 1
    return array(list)

if __name__ == '__main__':
    transE = TransE(margin=1)
    transE.transE(1000)

