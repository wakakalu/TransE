#!/usr/bin/python
# -*- coding: UTF-8 -*-
from numpy import *
import operator
import MySQLdb

class Predict:
    def __init__(self, recoNum = 10):
        self.userList = {}
        self.relationList = {}
        self.songList = {}
        self.tripleListTrain = []
        self.predictList = []
        self.recoNum = recoNum
        self.rank =[]

    def predict(self):
        if self.connectDB() == -1:
            return
        print("Succeed to connect datase")
        if self.loadData() == -1:
            return
        print("Succeed to load data")
        print("Start to predict...")
        self.getSongRank()
        print("Succeed to get song rank")
        if self.writeRankToDB() == -1:
            return 
        print("Succeed to write rank to databse")
        self.closeDB()

    def connectDB(self):
        try:
            self.dbc = MySQLdb.connect("localhost","root","","music3")
        except:
            print("Fail to connect database!!!!!!")
            return -1

    def closeDB(self):
        self.dbc.close()

    def writeRankToDB(self):
        print self.rank
        print("writing rank to database...")
        cursor = self.dbc.cursor()
        sql = "SELECT * FROM recommand"
        try:
            cursor.execute(sql)
            results = cursor.fetchall()
        except:
            print("Error:fail to write song rank of somebody to database")
            return -1
        if not results:#如果推荐表中无数据
            for r in self.rank:
                sql = "INSERT INTO recommand values (%d, '%s')"%(r[0], str(r[1]))
                try:
                    cursor.execute(sql)
                    self.dbc.commit()
                except:
                    self.dbc.rollback()
                    cursor.close()
                    print("Error:fail to write song rank of somebody to database")
                    return -1
        else:
            for r in self.rank:
                sql = "UPDATE recommand SET ranklist = '%s' WHERE id = %d"%(str(r[1]),r[0])
                try:
                    cursor.execute(sql)
                    self.dbc.commit()
                except:
                    self.dbc.rollback()
                    cursor.close()
                    print("Error:fail to write song rank of somebody to database")
                    return -1
        cursor.close()

    def getSongRank(self):
        cou = 0
        for predictUser in self.predictList:
            rankList = {}
            for songTemp in self.songList.keys():
                    corruptedTriplet = (predictUser, predictUser, songTemp)
                    if corruptedTriplet in self.tripleListTrain:
                        continue
                    rankList[songTemp] = distance(self.userList[predictUser], self.relationList[predictUser], self.songList[songTemp])
            nameRank = sorted(rankList.items(), key = operator.itemgetter(1))  #sorted返回一个元组组成的List
            x = 0
            rankList = []
            for i in nameRank:
                rankList.append(i[0])
                x += 1
                if x >= self.recoNum:
                    break
            self.rank.append((predictUser,rankList))
            cou += 1
            if cou % 10000 == 0:
                print(cou)

    def loadData(self):
        cursor = self.dbc.cursor()

        #读取用户向量表
        sql = "SELECT * FROM user_vector"
        try:
            cursor.execute(sql)
            allUsers = cursor.fetchall()
        except:
            print("Error: unable to fetch data")
            cursor.close()
            return -1
        for userVec in allUsers:
            self.userList[userVec[0]] = str2vec(userVec[1])

        #读取音乐向量表
        sql = "SELECT * FROM song_vector"
        try:
            cursor.execute(sql)
            allSongs = cursor.fetchall()
        except:
            print("Error: unable to fetch data")
            cursor.close()
            return -1
        for songVec in allSongs:
            self.songList[songVec[0]] = str2vec(songVec[1])

        #读取关系向量表
        sql = "SELECT * FROM relation_vector"
        try:
            cursor.execute(sql)
            allRelation = cursor.fetchall()
        except:
            print("Error: unable to fetch data")
            cursor.close()
            return -1
        for relationVec in allRelation:
            self.relationList[relationVec[0]] = str2vec(relationVec[1])

        #初始化训练列表
        sql = "SELECT * FROM songlike"
        try:
            cursor.execute(sql)
            allTriplets = cursor.fetchall()
        except:
            print("Error: unable to fetch data")
            cursor.close()
            return -1
        for triplet in allTriplets:
            self.tripleListTrain.append((triplet[1],triplet[1],triplet[2]))

        #初始化预测需要的用户表
        for triplet in allTriplets:
            if triplet[1] in self.predictList:
                continue
            else:
                self.predictList.append(triplet[1])
        cursor.close()        

def distance(h, r, t):
    s = h + r - t
    return linalg.norm(s)

def str2vec(str):
    vecList = [float(s) for s in str[1:-1].split(", ")]
    return array(vecList)

if __name__ == '__main__':
    predict = Predict()
    predict.predict()