#!/usr/bin/env python3
# encoding: utf-8
# @author: hoojo
# @email:    hoojo_@126.com
# @github:   https://github.com/hooj0
# @create date: 2018-07-03
# @copyright by hoojo @2018
# @changelog csv reader to writer new file


#===============================================================================
# 标题：csv reader to writer new file
#===============================================================================
# 描述：读写csv数据，将csv记录迁移
#-------------------------------------------------------------------------------


#-------------------------------------------------------------------------------
# 将csv记录迁移至新文件
#-------------------------------------------------------------------------------
import csv


class TransfromCsv(object):
    
    def reader(self, filename='target.csv'):
        csv_file = open(filename, 'r', encoding='ANSI')
        data = csv.reader(csv_file)
        
        seen_set, unseen_set = [], []
        for line in data:
            if line is None or line[0] == '':
                continue
            
            if line[-1] == 'y':
                seen_set.append(line)
            else:
                print("line: ", line)
                unseen_set.append(line)
            
        csv_file.close()
        return seen_set, unseen_set
    
    def writer(self, filename, data, mode='w'):
        csv_file = open(filename, mode, encoding='ANSI')
        writer = csv.writer(csv_file);
        
        for line in data:
            if len(line) <= 4:
                writer.writerow(line)
            else:
                writer.writerow(line[0:len(line) - 1])
        

if __name__ == '__main__':
    transform = TransfromCsv()
    seen_set, unseen_set = transform.reader('../douban-filter.csv')
    
    transform.writer('../movies.csv', seen_set, 'a')
    transform.writer('../douban-filter.csv', unseen_set)
    
    #transform.writer('../seen.csv', seen_set)
    #transform.writer('../unseen.csv', unseen_set)
    