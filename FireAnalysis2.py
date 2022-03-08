# Версия от 09.12.2021
import pandas as pd, numpy as np
from functools import partial  # вычисление мер центральной тенденции (среднее, мода, квантили и т.д.)
import FireAnalysis as fr # вспомогательные функции
import time
import pickle
import pyodbc as sqlMS # связь с Access

import seaborn as sns 
import matplotlib.pyplot as plt # подключение рyplot
import matplotlib.ticker as ticker # модуль управления метками (тиками)
import squarify   # диаграмма прямоугольник с областями
import re


def Report(Y, M):  #  Установить глобальные значения даты отчета
    global Yr, Mn
    Yr, Mn = Y, M


def openDB():  # Создать соединение с БД для модуля
    global connStr, conn, cursor
    connStr = (r"DRIVER={Microsoft Access Driver (*.mdb, *.accdb)};" r"DBQ=d:\FR\FR.accdb;") 
    conn = sqlMS.connect(connStr) # создать соединение с БД
    cursor = conn.cursor()


def closeDB():  # Закрыть соединение с БД для модуля
    # cursor.close() 
    # conn.close()  # закрытие соединения - получается, что лучше вообще закрывать соединение один раз в самом конце
    pass


def TimeOper(Y): # запрос к БД для вычисления среднего времени по показателям оперативного реагирования 

    # Y - год, количество месяцев определяется глобальной переменной - Mn

    strSQL = f'''
    SELECT F61, F62, F63, F64, F65, F66, F157, F158, F67, F68, F69, F70, F159, F160, т08F12_Объекты.ОбъектТактика, т02F6_ВидНасПункта.Тип, F1
    FROM т02F6_ВидНасПункта INNER JOIN (т08F12_Объекты INNER JOIN FR{Y} ON т08F12_Объекты.F12 = FR{Y}.F12) ON т02F6_ВидНасПункта.F6 = FR{Y}.F6 
    WHERE ((F4=0) AND ((F83+F84+F85+F86+F87)>0) AND ((F91+F92+F93)>0)  AND ([F71]=[F5]) AND (Month([F5])<={Mn}))
    '''
    openDB()
    df = pd.read_sql(strSQL, conn) # Запрос 
    closeDB()

    mask  = df.loc[:, ['F61', 'F63', 'F65', 'F157', 'F67', 'F69', 'F159']] > 23
    df[mask] = 0 # обнулить значения часов >23 - это Сибирко В.И. придумал, потому что в STP не получалось обрабатывать значения NaN
    
    df['Msg'] = 60*(df['F63']-df['F61']) + (df['F64']-df['F62'])             # Время сообщения о пожаре, мин.
    df['Arrive'] = 60*(df['F65']-df['F63']) + (df['F66']-df['F64'])          # Время прибытия первого пожарного подразделения, мин.
    df['Nazzle'] = 60*(df['F157']-df['F65']) + (df['F158']-df['F66'])        # Время подачи первого ствола, мин.
    df['FreeFire'] = 60*(df['F157']-df['F61']) + (df['F158']-df['F62'])      # Время свободного горения, мин.
    df['Localization'] = 60*(df['F67']-df['F157']) + (df['F68']-df['F158'])  # Время локализации пожара, мин.
    df['EndFire1'] = 60*(df['F69']-df['F67']) + (df['F70']-df['F68'])        # Время ликвидации открытого горения, мин.
    df['EndFire'] = 60*(df['F159']-df['F69']) + (df['F160']-df['F70'])       # Время ликвидации последствий пожара, мин.
    df['Busy'] = 60*(df['F159']-df['F63']) + (df['F160']-df['F64'])          # Время занятости на пожаре, мин.
    
    mask  = df.loc[:] < 0 # отрицательные значения - это переходящие значения
    df[mask] = np.NaN  # удаляем переходящие пожары, обычно их не больше 5%
    
    col = df.columns[:14]
    df = df.drop(col, axis = 1)

    return df # --------------------------------------


def TimeOperCurrent(Y):  # Опер показатели для отчетного года
    '''Запись оперативных показателей в файл'''
    TimeOper(Y).to_pickle('pkl/TimeOperCurrent.pkl')


def import_context(cnt):  # импорт словаря со всеми данными
    global context
    context = cnt


def DiagrOper(i, NameOper, NN, t):
    '''Диаграммы - показатели оперативного реагирования по Российской Федерации за пятилетний интервал. 
    1 – Прибытие первого пожарного подразделения, мин.; 2 – Свободное горение, мин.; 3 – Локализация, мин.; 
    4 - Ликвидация открытого горения, мин.
    - NN - номер диаграммы
    - NameOper - название диаграммы
    - i - индексы для словаря context
    - t - префикс названия индекса словаря
    '''
    with open('MeanOperIndex.pkl','rb') as inp:  # чтение словаря из файла
        context = pickle.load(inp)

    y1 = [context[f'{t}4n{i[j]}'] for j in range(5)] # прибытие
    y2 = [context[f'{t}4n{i[j]+6}'] for j in range(5)] # свободное горение
    y3 = [context[f'{t}4n{i[j]+9}'] for j in range(5)] # локализация
    y4 = [context[f'{t}4n{i[j]+12}'] for j in range(5)] # ликвидация открытого горения
    x= list(range(5))
    
    fig, ax = plt.subplots()
    fig.set_figwidth(16)    #  ширина и
    fig.set_figheight(12)   #  высота диаграммы
    
    ax.plot(x, y1, linestyle = '-', linewidth = 4, color = 'crimson', marker='o')
    ax.plot(x, y2, linestyle = '-', linewidth = 4, color = 'darkmagenta', marker='o')
    ax.plot(x, y3, linestyle = '-', linewidth = 4, color = 'indigo', marker='o')
    ax.plot(x, y4, linestyle = '-', linewidth = 4, color = 'darkblue', marker='o')
    
    ax.set_ylim([5, 18]) # лимит оси Y 
    plt.title(NameOper, fontsize=16, pad= 10) # pad - зазор между заголовком и полем диаграммы
    ax.set_xlabel('Годы', fontsize = '14') # название оси X
    ax.set_ylabel('Время, мин.', fontsize = '14') # название оси Y
    ax.xaxis.set_major_locator(ticker.MultipleLocator(1)) #  интервал основных делений
    ax.grid(which='major', color = 'paleturquoise')  # цвет сетки
    labels = [Yr-4+i for i in range(5)]
    plt.xticks(x, labels) # замена названий меток по оси X   
    for i in range(5): # подписи значений точек 
        plt.text(x[i], y1[i]+0.1, y1[i], fontsize=10, color='dimgray')
        plt.text(x[i], y2[i]+0.1, y2[i], fontsize=10, color='dimgray')
        plt.text(x[i], y3[i]+0.1, y3[i], fontsize=10, color='dimgray')
        plt.text(x[i], y4[i]+0.1, y4[i], fontsize=10, color='dimgray')
    
    # аннотации со стрелками и номерами
    plt.annotate(1, xy=(0, y1[0]),  xycoords='data', xytext=(-0.15, y1[0]+0.35), textcoords='data', arrowprops=dict(arrowstyle='->'), fontsize = 16, fontweight='bold')
    plt.annotate(2, xy=(0, y2[0]),  xycoords='data', xytext=(-0.15, y2[0]+0.35), textcoords='data', arrowprops=dict(arrowstyle='->'), fontsize = 16, fontweight='bold')
    plt.annotate(3, xy=(0, y3[0]),  xycoords='data', xytext=(-0.15, y3[0]+0.35), textcoords='data', arrowprops=dict(arrowstyle='->'), fontsize = 16, fontweight='bold')
    plt.annotate(4, xy=(0, y4[0]),  xycoords='data', xytext=(-0.15, y4[0]+0.35), textcoords='data', arrowprops=dict(arrowstyle='->'), fontsize = 16, fontweight='bold')
    
    fig.savefig(f'img/img-{NN}', bbox_inches = 'tight', dpi=300) # сохранить диаграмму в файл


def DiagrOper1(i, NameOper, NN, t):  # Среднее время ликвидации последствий пожара ----------------------

    with open('MeanOperIndex.pkl','rb') as inp:  # чтение словаря из файла
        context = pickle.load(inp)

    y1 = [context[f'{t}4n{i[j]}'] for j in range(5)] # ликвидация последствий пожара
    y2 = [context[f'{t}4n{i[j]+1}'] for j in range(5)] # - город
    y3 = [context[f'{t}4n{i[j]+2}'] for j in range(5)] # - село
    x= list(range(5))
    
    fig, ax = plt.subplots()
    fig.set_figwidth(16)    #  ширина и
    fig.set_figheight(12)   #  высота диаграммы
    
    ax.plot(x, y1, linestyle = '-', linewidth = 4, color = 'crimson', marker='o')
    ax.plot(x, y2, linestyle = '-', linewidth = 4, color = 'darkmagenta', marker='o')
    ax.plot(x, y3, linestyle = '-', linewidth = 4, color = 'indigo', marker='o')
    
    ax.set_ylim([10, 70]) # лимит оси Y 
    plt.title(NameOper, fontsize=16, pad= 10) # pad - зазор между заголовком и полем диаграммы
    ax.set_xlabel('Годы', fontsize = '14') # название оси X
    ax.set_ylabel('Время, мин.', fontsize = '14') # название оси Y
    ax.xaxis.set_major_locator(ticker.MultipleLocator(1)) #  интервал основных делений
    ax.grid(which='major', color = 'paleturquoise')  # цвет сетки
    labels = [Yr-4+i for i in range(5)]
    plt.xticks(x, labels) # замена названий меток по оси X   
    for i in range(5): # подписи значений точек 
        plt.text(x[i], y1[i]+0.8, y1[i], fontsize=10, color='dimgray')
        plt.text(x[i], y2[i]+0.8, y2[i], fontsize=10, color='dimgray')
        plt.text(x[i], y3[i]+0.8, y3[i], fontsize=10, color='dimgray')
    
    # аннотации со стрелками и номерами
    plt.annotate(1, xy=(0, y1[0]),  xycoords='data', xytext=(-0.15, y1[0]+1.85), textcoords='data', arrowprops=dict(arrowstyle='->'), fontsize = 16, fontweight='bold')
    plt.annotate(2, xy=(0, y2[0]),  xycoords='data', xytext=(-0.15, y2[0]+1.85), textcoords='data', arrowprops=dict(arrowstyle='->'), fontsize = 16, fontweight='bold')
    plt.annotate(3, xy=(0, y3[0]),  xycoords='data', xytext=(-0.15, y3[0]+1.85), textcoords='data', arrowprops=dict(arrowstyle='->'), fontsize = 16, fontweight='bold')
    
    fig.savefig(f'img/img-{NN}', bbox_inches = 'tight', dpi=300) # сохранить диаграмму в файл


def DiagrOper2(i, NameOper, NN, t):  # Среднее время занятости на пожаре ----------------------

    with open('MeanOperIndex.pkl','rb') as inp:  # чтение словаря из файла
        context = pickle.load(inp)

    y1 = [context[f'{t}4n{i[j]}'] for j in range(5)] # ликвидация последствий пожара
    y2 = [context[f'{t}4n{i[j]+1}'] for j in range(5)] # - город
    y3 = [context[f'{t}4n{i[j]+2}'] for j in range(5)] # - село
    x= list(range(5))
    
    fig, ax = plt.subplots()
    fig.set_figwidth(16)    #  ширина и
    fig.set_figheight(12)   #  высота диаграммы
    
    ax.plot(x, y1, linestyle = '-', linewidth = 4, color = 'crimson', marker='o')
    ax.plot(x, y2, linestyle = '-', linewidth = 4, color = 'darkmagenta', marker='o')
    ax.plot(x, y3, linestyle = '-', linewidth = 4, color = 'indigo', marker='o')
    
    ax.set_ylim([25, 110]) # лимит оси Y --------------------
    plt.title(NameOper, fontsize=16, pad= 10) # pad - зазор между заголовком и полем диаграммы
    ax.set_xlabel('Годы', fontsize = '14') # название оси X
    ax.set_ylabel('Время, мин.', fontsize = '14') # название оси Y
    ax.xaxis.set_major_locator(ticker.MultipleLocator(1)) #  интервал основных делений
    ax.grid(which='major', color = 'paleturquoise')  # цвет сетки
    labels = [Yr-4+i for i in range(5)]
    plt.xticks(x, labels) # замена названий меток по оси X   
    for i in range(5): # подписи значений точек 
        plt.text(x[i], y1[i]+0.8, y1[i], fontsize=10, color='dimgray')
        plt.text(x[i], y2[i]+0.8, y2[i], fontsize=10, color='dimgray')
        plt.text(x[i], y3[i]+0.8, y3[i], fontsize=10, color='dimgray')
    
    # аннотации со стрелками и номерами
    plt.annotate(1, xy=(0, y1[0]),  xycoords='data', xytext=(-0.15, y1[0]+1.85), textcoords='data', arrowprops=dict(arrowstyle='->'), fontsize = 16, fontweight='bold')
    plt.annotate(2, xy=(0, y2[0]),  xycoords='data', xytext=(-0.15, y2[0]+1.85), textcoords='data', arrowprops=dict(arrowstyle='->'), fontsize = 16, fontweight='bold')
    plt.annotate(3, xy=(0, y3[0]),  xycoords='data', xytext=(-0.15, y3[0]+1.85), textcoords='data', arrowprops=dict(arrowstyle='->'), fontsize = 16, fontweight='bold')
    
    fig.savefig(f'img/img-{NN}', bbox_inches = 'tight', dpi=300) # сохранить диаграмму в файл


def Chart__10():  # Диаграмма с областями - общие показатели оперативного реагирования

    with open('MeanOperIndex.pkl','rb') as inp:  # чтение словаря из файла
        context = pickle.load(inp)

    n = [3,6,12,15,18]
    dd = pd.DataFrame([context[f't4n{j}'] for j in range(1, 102, 25)]) # сообщение 
    for i in n:
        dd1 = pd.DataFrame([context[f't4n{j}'] for j in range(1+i, 102+i, 25)])
        dd = pd.concat([dd, dd1], axis=1) 

    rng = np.arange(5)
    # rnd = np.random.randint(0, 5, size=(5, rng.size)) # randint
    # rnd = np.random.sample((5, 5))
    rnd = dd.to_numpy().T
    yrs = Yr-4 + rng
    mycolors = ['tab:red', 'tab:blue', 'tab:green', 'tab:orange', 'tab:pink', 'tab:olive']   # , 'tab:pink', 'tab:olive'

    fig, ax = plt.subplots(1, 1, figsize = (16, 12), dpi = 80)
    ax.stackplot(yrs, rnd, labels=['1) Сообщение, мин.', '2) Прибытие, мин.','3) 1-й ствол, мин.','4) Локализация, мин.','5) Ликвидация, мин.','6) Ликвидация последствий, мин.'],
                colors=mycolors, edgecolor='w', linewidth = 2)
    ax.set_title('Общая динамика средних показателей оперативного реагирования', fontsize=16, pad= 10)
    ax.legend(loc='upper left', fontsize=10, ncol=3) # положение и формат легенды
    ax.set_ylabel('Время, мин.', size=12)
    ax.set_xlabel('Годы', size=12)
    ax.set_xlim(xmin=yrs[0], xmax=yrs[-1])
    ax.set(ylim=[0, 75])
    ax.xaxis.set_major_locator(ticker.MultipleLocator(1)) #  интервал основных делений
    ax.grid(which='major', color = 'paleturquoise')  # цвет сетки
    # fig.tight_layout()  # применяется к объекту Figure в целом для очистки пробелов

    # аннотации со стрелками и номерами
    y1 = rnd[0,-1]
    plt.annotate(1, xy=(yrs[4]-0.06, y1-1),  xycoords='data', xytext=(yrs[4]+0.08, y1-0.8), textcoords='data', arrowprops=dict(arrowstyle='->'), fontsize = 16, fontweight='bold')
    y1 = y1 + rnd[1,-1]
    plt.annotate(2, xy=(yrs[4]-0.06, y1-2.5),  xycoords='data', xytext=(yrs[4]+0.08, y1-1), textcoords='data', arrowprops=dict(arrowstyle='->'), fontsize = 16, fontweight='bold')
    y1 = y1 + rnd[2,-1]
    plt.annotate(3, xy=(yrs[4]-0.06, y1-1),  xycoords='data', xytext=(yrs[4]+0.08, y1-0.8), textcoords='data', arrowprops=dict(arrowstyle='->'), fontsize = 16, fontweight='bold')
    y1 = y1 + rnd[3,-1]
    plt.annotate(4, xy=(yrs[4]-0.06, y1-2.5),  xycoords='data', xytext=(yrs[4]+0.08, y1-0.8), textcoords='data', arrowprops=dict(arrowstyle='->'), fontsize = 16, fontweight='bold')
    y1 = y1 + rnd[4,-1]
    plt.annotate(5, xy=(yrs[4]-0.06, y1-3.5),  xycoords='data', xytext=(yrs[4]+0.08, y1-0.8), textcoords='data', arrowprops=dict(arrowstyle='->'), fontsize = 16, fontweight='bold')
    y1 = y1 + rnd[5,-1]
    plt.annotate(6, xy=(yrs[4]-0.06, y1-3.5),  xycoords='data', xytext=(yrs[4]+0.08, y1-0.8), textcoords='data', arrowprops=dict(arrowstyle='->'), fontsize = 16, fontweight='bold')

    fig.savefig('img/img-010', bbox_inches = 'tight', dpi=300) # сохранить диаграмму в файл


def Arrive_FederalCities():  # Время прибытия для городов федерального значения
    openDB()

    strSQL = f'''
    SELECT FR{Yr}.F1, ([F65]*60+[F66])-([F63]*60+[F64]) AS Arrive
    FROM FR{Yr}
    WHERE (((FR{Yr}.F1)=141 Or (FR{Yr}.F1)=1145 Or (FR{Yr}.F1)=1167) AND ((FR{Yr}.F4)=0) AND (([F83]+[F84]+[F85]+[F86]+[F87])>0) AND ((([F65]*60+[F66])-([F63]*60+[F64]))>=0 And (([F65]*60+[F66])-([F63]*60+[F64]))<100) AND ((Month([F5]))<={Mn}));
    '''
    df = pd.read_sql(strSQL, conn) # Запрос 

    return df


def Chart__11():  #  Диаграмма - Время следования на пожар - Москва, СПб и Севастополь

    df = Arrive_FederalCities()

    dy1 = df['Arrive'][df.F1 == 1145].value_counts().sort_index(ascending=True) # Москва
    dy2 = df['Arrive'][df.F1 == 141].value_counts().sort_index(ascending=True) # СПб
    dy3 = df['Arrive'][df.F1 == 1167].value_counts().sort_index(ascending=True) # Севастополь

    x1 = dy1.index - 0.4 #  Задаем смещение равное половине ширины прямоугольника
    y1 = dy1.values
    x2 = dy2.index
    y2 = dy2.values
    x3 = dy3.index + 0.4
    y3 = dy3.values

    fig, ax = plt.subplots(1, 1, figsize = (12, 8), dpi = 80)

    ax.bar(x1, y1, width = 0.4, edgecolor = 'darkblue', linewidth = 1, label = ' - г. Москва')
    ax.bar(x2, y2, width = 0.4, edgecolor = 'darkblue', linewidth = 1, label = ' - г. Санкт-Петербург')
    ax.bar(x3, y3, width = 0.4, edgecolor = 'darkblue', linewidth = 1, label = ' - г. Севастополь')
    ax.set_xlim(xmin=0, xmax=30)
    ax.set_title('Распределение частот показателя - Время следования на пожар', fontsize=16, pad= 10)
    ax.legend(loc='upper right', fontsize=14, facecolor = 'floralwhite') # положение и формат легенды
    ax.set_ylabel('Частота, ед.', size=12)
    ax.set_xlabel('Время, мин.', size=12)
    ax.grid(which='major', color = 'paleturquoise')  # цвет сетки
    # ax.set_facecolor('seashell')  # цвет заливки области диаграммы
    # fig.set_facecolor('floralwhite')  # цвет заливки рамки
    fig.tight_layout()  # применяется к объекту Figure в целом для очистки пробелов

    closeDB()

    fig.savefig('img/img-011', bbox_inches = 'tight', dpi=300) # сохранить диаграмму в файл


def BaseOper2016_2020():  # Базовые показатели оперативного реагирования за 5 лет (2016-2020 годы)
    strSQL = '''
    SELECT F1, т02F6_ВидНасПункта.Тип, F61, F62, F63, F64, F65, F66, F157, F158, F67, F68, F69, F70, F159, F160, F5, F71
    FROM т02F6_ВидНасПункта INNER JOIN (т08F12_Объекты INNER JOIN FR2020 ON т08F12_Объекты.F12 = FR2020.F12) ON т02F6_ВидНасПункта.F6 = FR2020.F6
    WHERE ((F4=0) AND (([F83]+[F84]+[F85]+[F86]+[F87])>0) AND (([F91]+[F92]+[F93])>0) AND ([F71]=[F5]))

    UNION ALL
    SELECT F1, т02F6_ВидНасПункта.Тип, F61, F62, F63, F64, F65, F66, F157, F158, F67, F68, F69, F70, F159, F160, F5, F71
    FROM т02F6_ВидНасПункта INNER JOIN (т08F12_Объекты INNER JOIN FR2019 ON т08F12_Объекты.F12 = FR2019.F12) ON т02F6_ВидНасПункта.F6 = FR2019.F6
    WHERE ((F4=0) AND (([F83]+[F84]+[F85]+[F86]+[F87])>0) AND (([F91]+[F92]+[F93])>0) AND ([F71]=[F5]))

    UNION ALL
    SELECT F1, т02F6_ВидНасПункта.Тип, F61, F62, F63, F64, F65, F66, F157, F158, F67, F68, F69, F70, F159, F160, F5, F71
    FROM т02F6_ВидНасПункта INNER JOIN (т08F12_Объекты INNER JOIN FR2018 ON т08F12_Объекты.F12 = FR2018.F12) ON т02F6_ВидНасПункта.F6 = FR2018.F6
    WHERE ((F4=0) AND (([F83]+[F84]+[F85]+[F86]+[F87])>0) AND (([F91]+[F92]+[F93])>0) AND ([F71]=[F5]))

    UNION ALL
    SELECT F1, т02F6_ВидНасПункта.Тип, F61, F62, F63, F64, F65, F66, F157, F158, F67, F68, F69, F70, F159, F160, F5, F71
    FROM т02F6_ВидНасПункта INNER JOIN (т08F12_Объекты INNER JOIN FR2017 ON т08F12_Объекты.F12 = FR2017.F12) ON т02F6_ВидНасПункта.F6 = FR2017.F6
    WHERE ((F4=0) AND (([F83]+[F84]+[F85]+[F86]+[F87])>0) AND (([F91]+[F92]+[F93])>0) AND ([F71]=[F5]))

    UNION ALL
    SELECT F1, т02F6_ВидНасПункта.Тип, F61, F62, F63, F64, F65, F66, F157, F158, F67, F68, F69, F70, F159, F160, F5, F71
    FROM т02F6_ВидНасПункта INNER JOIN (т08F12_Объекты INNER JOIN FR2016 ON т08F12_Объекты.F12 = FR2016.F12) ON т02F6_ВидНасПункта.F6 = FR2016.F6
    WHERE ((F4=0) AND (([F83]+[F84]+[F85]+[F86]+[F87])>0) AND (([F91]+[F92]+[F93])>0) AND ([F71]=[F5]));
    '''

    openDB()
    df = pd.read_sql(strSQL, conn) # Запрос 
    closeDB()

    col = ['F61', 'F63', 'F65', 'F157', 'F67', 'F69', 'F159']
    for i in col:
        mask  = df[i] > 23
        df.loc[mask, i] = 0  # обнулить значения часов >23 - это Сибирко В.И. придумал, потому что в STP не получалось обрабатывать значения NaN

    df['Msg'] = 60*(df['F63']-df['F61']) + (df['F64']-df['F62'])             # Время сообщения о пожаре, мин.
    df['Arrive'] = 60*(df['F65']-df['F63']) + (df['F66']-df['F64'])          # Время прибытия первого пожарного подразделения, мин.
    df['Nazzle'] = 60*(df['F157']-df['F65']) + (df['F158']-df['F66'])        # Время подачи первого ствола, мин.
    df['FreeFire'] = 60*(df['F157']-df['F61']) + (df['F158']-df['F62'])      # Время свободного горения, мин.
    df['Localization'] = 60*(df['F67']-df['F157']) + (df['F68']-df['F158'])  # Время локализации пожара, мин.
    df['EndFire1'] = 60*(df['F69']-df['F67']) + (df['F70']-df['F68'])        # Время ликвидации открытого горения, мин.
    df['EndFire'] = 60*(df['F159']-df['F69']) + (df['F160']-df['F70'])       # Время ликвидации последствий пожара, мин.
    df['Busy'] = 60*(df['F159']-df['F63']) + (df['F160']-df['F64'])          # Время занятости на пожаре, мин.

    col = ['Msg', 'Arrive', 'Nazzle', 'FreeFire', 'Localization', 'EndFire1', 'EndFire', 'Busy']
    for i in col:
        mask  = df[i] < 0
        df.loc[mask, i] = np.NaN

    df = df.drop(df.columns[2:18], axis = 1)
    df.to_pickle('pkl/BaseOper2016_2020.pkl')   # Сохранить данные в файл


def Annex__1(): # Таблица П.1 -------сводная пожары по регионам и объектам ------------------------------------------------------------
    context = {}
    strSQL = f'''
    TRANSFORM Count(TT.[F12]) AS [Count-F12]
    SELECT TT.[РегионФ], Count(TT.[F12]) AS [Итог]
    FROM (
    SELECT т01_F1_Регион.РегионФ, FR{Yr}.F12, т08F12гр_ОбъектыГр.ОбъектГруппировка
    FROM т08F12гр_ОбъектыГр INNER JOIN (т08F12_Объекты INNER JOIN (т01_F1_Регион INNER JOIN FR{Yr} ON т01_F1_Регион.F1 = FR{Yr}.F1) ON т08F12_Объекты.F12 = FR{Yr}.F12) ON т08F12гр_ОбъектыГр.F12гр = т08F12_Объекты.F12гр
    WHERE (((FR{Yr}.F4)=0) AND ((Month([F5]))<={Mn}))
    ) AS TT
    GROUP BY TT.[РегионФ]
    PIVOT TT.[ОбъектГруппировка]
    '''
    openDB()
    cursor.execute(strSQL)
    df, i = pd.DataFrame(np.zeros((86, 10))), -1
    a = ['a' for _ in range(86)] 
    for row in cursor.fetchall():
        i +=1
        a[i] = row[0]
        for j in range(1, 10):
            df[j][i] = row[j]

    df[0] = a # названия в первой колонке   
    df = df.fillna(0) # замена значений NaN на нули

    context = {f'pr1n{i}' : fr.F(df[i//86][i%86]) for i in range(860)}
    closeDB()

    print('Annex__1:', time.strftime("%H:%M:%S", time.localtime()))
    return(context)


def RegionsNames():  # Код и название региона РФ
    '''Код и название региона РФ'''
    strSQL = f'''
    SELECT т01_F1_Регион.F1, т01_F1_Регион.РегионФ
    FROM т01_F1_Регион
    '''
    openDB()
    RegionsName = pd.read_sql(strSQL, conn) # Запрос 
    closeDB()

    return RegionsName


def Oper95(Ind95, SV=0):  # функция вычисляет базовые меры центральной тенденции для 2016-2020 годов и отдельно за отчетный год
    '''Базовые значения за 2016-2020 гг должны быть в фрейме df
    Данные за отчетный год в фрейме dft
    Ind95 - может быть равен: Msg, Arrive, Nazzle, FreeFire, Localization, EndFire1, EndFire, Busy
    SV - признак отбора данных для городов (SV=1) и сел (SV=2), если SV не указать, то будет рассчитываться обобщенный показатель
    '''
    RegionsName = RegionsNames()  # Код и название региона РФ
    df = pd.read_pickle('pkl/BaseOper2016_2020.pkl')
    dft = pd.read_pickle('pkl/TimeOperCurrent.pkl')

    q_95 = partial(pd.Series.quantile, q=0.95) # возвращает обертку над pd.Series.quantile()
    q_95.__name__ = '95%' # пойдет в наименование будущего столбца
    
    agg_func = {
        Ind95: [q_95, 'mean', 'median', 'min', 'max'] # percentile_25, lambda_25, lambda x: x.quantile(.95)
    }
    if SV == 0: # обобщенный показатель
        Msg1 = df.groupby(['F1']).agg(agg_func).round(2) # Базовый показатель
    else:  #  показатель для городов или села
        Msg1 = df[df['Тип']==SV].groupby(['F1']).agg(agg_func).round(2) # Базовый показатель
        
    Msg1.columns.set_levels(['Msg'],level=0, inplace=True) # переимновать мультииндекс
    
    agg_func1 = {
        Ind95: [q_95, 'mean', 'median'] # percentile_25, lambda_25, lambda x: x.quantile(.95)
    }
    if SV == 0:  # обобщенный показатель
        Msg2 = dft.groupby(['F1']).agg(agg_func1).round(2) # Показатель - отчетный год
    else:  #  показатель для городов или села
        Msg2 = dft[dft['Тип']==SV].groupby(['F1']).agg(agg_func1).round(2)
        
    Msg2.columns.set_levels(['MsgT'],level=0, inplace=True) # переименовать мультииндекс
    Msg1 = pd.merge(Msg1, Msg2, how ='inner', on ='F1') # объединение фреймов
    
    Msg1[('Tmp','t95')] = round((Msg1[('MsgT','95%')] - Msg1[('Msg','95%')]) / Msg1[('Msg','95%')],2)  # прирост по 95%-интервалу
    Msg1[('Tmp','mean')] = round((Msg1[('MsgT','mean')] - Msg1[('Msg','mean')]) / Msg1[('Msg','mean')],2)  # прирост по среднему
    Msg1[('Msg','Reg')] = Msg1.index.map(RegionsName.set_index('F1')['РегионФ'])  # вставить столбец с названиями регионов
    Msg1 = Msg1.iloc[Msg1[('Msg','Reg')].str.lower().argsort()]  # сортировка без учета регистра
    # # изменение порядка столбцов
    col = ([('Msg','Reg'), ('Msg','95%'), ('Msg','mean'),('Msg','median'), ('Msg','min'),('Msg','max'),('MsgT','95%'), ('MsgT','mean'), ('MsgT', 'median'), ('Tmp','t95'),('Tmp','mean')])
    Msg1 = Msg1[col]
    return(Msg1)

def tab_5(ctxt): # Таблица 5. РТП первый и старший по должности
    with open('pkl\BasicInd.pkl', 'rb') as filehandle:  # Официальные данные за два года
        BasicInd = pickle.load(filehandle)

    
    strSQL = f'''TRANSFORM Count(Q2.[F109]) AS [CF109]
    SELECT Q2.[Статус]
    FROM (SELECT т27F109_110_РТП.Статус, Q.St, Q.F109
    FROM т27F109_110_РТП INNER JOIN (SELECT FR{Yr}.F109, 1 AS St
    FROM FR{Yr}
    WHERE (((Month([F5]))<={Mn}))

    UNION ALL
    SELECT FR2021.F110, 2 AS St
    FROM FR{Yr}
    WHERE (((Month([F5]))<={Mn})))  AS Q ON т27F109_110_РТП.F109 = Q.F109) AS Q2
    GROUP BY Q2.[Статус]
    PIVOT Q2.[St];'''

    df = pd.read_sql(strSQL, conn)  # заполнение фрейма
    df.insert(2, 'd1', round(df['1'] / BasicInd[1][2] * 100, 1))  # добавление столбца на вторую позицию
    df['d2'] = round(df['2'] / BasicInd[1][2] * 100, 1)  # добавление столбца справа
    dc, y = {}, 0  # временный словарь и счетчик
    for i in range(1, df.shape[1]): # цикл по столбцам
        for j in range(len(df)):  # цикл по строкам
            if (i==1 or i==3): 
                x = round(df.iloc[j][i])  # целые значения 
            else:
                x = df.iloc[j][i]  # доля
            dc[f't5n{y}'] = str(x)  # добавление элемента в словарь
            y += 1
    ctxt = {**ctxt, **dc}
    return ctxt
 
    
def tab_6(ctxt):  # Таблица 6 – Старший по должности РТП за период с 2010 по отчетный год

    df1 = pd.DataFrame([1,2,3,4,5,6,7,10,20], columns=['Статус'])   # фрейм для сохранения результатов запроса
    df1 = df1.set_index('Статус')  #  назначить индексный столбец

    for j in range(2010, Yr+1):
        strSQL = f'''SELECT т27F109_110_РТП.Статус, Count(FR{j}.F110) AS Ct
                    FROM т27F109_110_РТП INNER JOIN FR{j} ON т27F109_110_РТП.F109 = FR{j}.F110
                    WHERE (((Month([F5]))<={Mn}) AND ((FR{j}.F4)=0) AND ((т27F109_110_РТП.Статус)<30))
                    GROUP BY т27F109_110_РТП.Статус'''

        df = pd.read_sql(strSQL, conn)  # возможно надо будет выполнять проверку, чтобы были заполнены данные по всем категориям
        df = df.set_index('Статус')
        
        df1 = df1.merge(df, left_on='Статус', right_on='Статус', how='outer')
        
    df1 = df1.fillna(0).astype(int) # удаление NaN и преобразование всего фрейма в тип int
    df1.columns = list(range(2010, Yr+1))
    df1 = df1.T
    df1.columns = list(range(1, 10))
    df1.to_pickle('pkl\RTP2.pkl')  #  сохранить результаты запроса в файл


def chart_12():  # РТП старший по должности 
    df = pd.read_pickle('pkl\RTP2.pkl')
    
    # Decide Colors 
#     mycolors = ['tab:red', 'tab:blue', 'tab:green', 'tab:orange', 'tab:brown', 'tab:grey', 'tab:pink', 'tab:olive']  # olive
    prop_cycle = plt.rcParams['axes.prop_cycle']
    mycolors = prop_cycle.by_key()['color']

    # Draw Plot and Annotate
    fig, ax = plt.subplots(1,1,figsize=(16, 9), dpi= 300)
    columns = df.columns
    labs = columns.values.tolist()

    # Prepare data
    x  = df.index.values.tolist()
    y1 = df[1].values.tolist()
    y2 = df[2].values.tolist()
    y3 = df[3].values.tolist()
    y4 = df[4].values.tolist()
    y5 = df[5].values.tolist()
    y6 = df[6].values.tolist()
    y7 = df[7].values.tolist()
    y8 = df[8].values.tolist()
    y9 = df[9].values.tolist()
    y = np.vstack([y1, y2, y3, y4, y5, y6, y7, y8, y9])

    # Plot for each column
    labs = columns.values.tolist()
    ax = plt.gca()
    ax.stackplot(x, y, labels=labs, colors=mycolors, alpha=0.8)

    # Decorations
    ax.set_title('Категории РТП', fontsize=20)
    ax.set(ylim=[0, 3.5*10**5])
    ax.legend(fontsize=12, ncol=4, loc='upper left', facecolor = 'oldlace') # параметры легенды
    plt.xticks(x[::2], fontsize=16, horizontalalignment='center')  # подписи горизонтальной оси
    plt.yticks(np.arange(0, 3.5*10**5, 5*10**4), fontsize=12)  # масштаб вертикальной оси
    plt.xlim(x[0], x[-1])  # масштаб горизонтальной оси (от первого до последнего элемента)
    plt.ylabel('Количество пожаров, ед.', fontsize=16)
    plt.xlabel('Годы', fontsize=14)
    plt.grid()

    # Lighten borders
    plt.gca().spines["top"].set_alpha(0)
    plt.gca().spines["bottom"].set_alpha(.3)
    plt.gca().spines["right"].set_alpha(0)
    plt.gca().spines["left"].set_alpha(.3)
    
    # аннотации со стрелками и номерами
    #  координаты для стрелок
    i1 = 0
    dy = df.tail(1)
    dy = dy.append(dy//2).append(dy*0)
    for i in range(0, dy.shape[1]):
        i1 += dy.iat[0,i]
        dy.iat[2,i] = i1 - dy.iat[1,i]

    # расставить стрелки
    for i in range(6):
        plt.annotate(i+1, xy=(Yr-0.1, dy.iat[2,i]),  xycoords='data', xytext=(Yr+0.3, dy.iat[2,i]*1.2), textcoords='data', arrowprops=dict(arrowstyle='->'), fontsize = 16, fontweight='bold')
    
    fig.savefig('img/img-012', bbox_inches = 'tight', dpi=300) # сохранить диаграмму в файл

    plt.show()

    
def chart_13():  # Диаграмма - Объекты, потушенные ДПД
    strSQL = f'''SELECT т08F12гр_ОбъектыГр.ОбъектГр AS Head1, Count(FR{Yr}.F75) AS Numbers
    FROM т08F12гр_ОбъектыГр INNER JOIN (т08F12_Объекты INNER JOIN FR{Yr} ON т08F12_Объекты.F12 = FR{Yr}.F12) ON т08F12гр_ОбъектыГр.F12гр = т08F12_Объекты.F12гр
    WHERE (((FR{Yr}.F75)=6) AND ((FR{Yr}.F4)=0) AND ((Month([F5]))<={Mn}))
    GROUP BY т08F12гр_ОбъектыГр.ОбъектГр ORDER BY Count(FR{Yr}.F75) DESC;
    '''
    df = pd.read_sql(strSQL, conn)  # заполнение фрейма
    
    other, n = 0, 1  #  прочие объекты, счетчик
    ob = ''
    list_del, list_head = [], []  # список на удаление строк и список названий показателей

    for i in range(df.shape[0]):
        if (df.iat[i,1] < 10) or (df.iat[i,0].find('Другие')==0):
            other += df.iat[i,1]
            list_del.append(i)
        else:
            list_head.append(df.iat[i,0])
            ob += str(n) + '. ' + df.iat[i,0] + '; '
            n += 1
    print('Объекты пожаров потушенных силами добровольцев', ob)

    df = df.drop(list_del)
    df_l = df.tail(1).copy()
    df_l.iat[0,0], df_l.iat[0,1] = 'Прочие', other
    df2 = df.append(df_l, ignore_index = True)
    df2['Head1'] = (df2.index+1)
    df2['Head1'] = df2['Head1'].astype(str)
    df2.iat[0,0] = '1. Открытые территории'
    df2.iat[1,0] = '2. Жилье'
    df2.iat[df2.shape[0]-1,0] = str(df2.shape[0])+'. Прочие'
    
    df = df2
    labels = df.apply(lambda x: str(x[0]) + "\n (" + str(x[1]) + ")", axis=1)
    sizes = df['Numbers'].values.tolist()
    colors = [plt.cm.Spectral(i/float(len(labels))) for i in range(len(labels))]

    # Draw Plot
#     fig, ax = plt.subplots(1,1,figsize=(16, 9), dpi= 300)
    plt.figure(figsize=(12,8), dpi= 300)
    squarify.plot(sizes=sizes, label=labels, color=colors, alpha=.8)

    # Decorate
    plt.title('Объекты пожаров, потушенных силами ДПД')
    plt.axis('off')
    
    plt.savefig('img/img-013', bbox_inches = 'tight', dpi=300) # сохранить диаграмму в файл

def Village(nm):
    '''Поиск признаков сельского населенного пункта'''
    patt = [r'\bс\.',r'(?i)\bсел[оа]\b',r'(?i)\bдеревн.|\bдер\.|\bд\.\s*[А-Я]{2}', r'\bп\.', r'(?i)\bпос\.|\bпос[её]лок',
        r'(?i)\bсельск\w+\s+поселени\w+',r'\bс[А-Я]',r'\bп[А-Я]',r'\bх\.|\bхут\.|\bхутор|\bх\s?[-–]\s?р',
        r'(?i)\bст\.|\bстаниц.|\bст\s?[-–]\s?ц.',r'(?i)\bпгт\.?',r'\bСНТ',r'\bаул',r'\bЗАТО\b',r'(?i)\bулус\b',
        r'(?i)\bкишлак\b',r'(?i)\bслобода\b',r'(?i)\bулус\b',r'(?i)\bразъезд\b',r'(?i)\bсовхоз\b',r'(?i)\bколхоз\b']
    a = False
    for i in patt:
        if re.search(i, nm) != None:
            a = True
            break
    return a

def del_null(dfn): 
    '''Проверка фреймов на наличие nan и удаление'''
    if dfn.isnull().values.any():
        dfn = dfn.fillna(0)
        return dfn.astype(int, errors='ignore')
    return dfn


def Chart_14():  # Диаграмма - Распределение пожаров по времени занятости (по городам и селам)
    strSQL = f'''
    SELECT FR{Yr}.F6, FR{Yr}.F5, FR{Yr}.F61, FR{Yr}.F62, FR{Yr}.F159, FR{Yr}.F160, FR{Yr}.F71
    FROM FR{Yr}
    WHERE (((FR{Yr}.F75)<>0 And (FR{Yr}.F75)<>11) AND (([F43]+[F44])>0) AND (([F91]+[F92]+[F93])>0) AND (([F83]+[F84]+[F85]+[F86]+[F87])>0) AND ((FR{Yr}.F4)=0) AND ((Month([F5]))<={Mn}));
    '''
    openDB()
    df = pd.read_sql(strSQL, conn)
    df.F5 = pd.to_datetime(df.F5)  # форматирование полей, содержащих даты
    df.F71 = pd.to_datetime(df.F71)

    
    # Продолжительность пожара
    df['Duration'] = ((df.F71 - df.F5).dt.days * 1440 + (df.F159 - df.F61) * 60 + (df.F160 - df.F62))
    df_del  = df.Duration < 1  # удаление строк где продолжительность пожара менее 10 мин 
    df = df[~df_del]

    # Преобразование типов населенных пунктов: 1 - города и ПГТ; 2 - все остальные
    mask  = df.F6 == 2 # поселки городского типа
    df.loc[mask, 'F6'] = 1  # приравнять к городам
    mask  = df.F6 != 1 # все населенные пункты НЕ города
    df.loc[mask, 'F6'] = 2  # сделать единого типа - 2

    df =  df.astype(int, errors='ignore')  # Понижение разрядности показателей

    ## Диаграмма ==============
    #Города и села
    dg1 = df[df.F6 == 1]
    dg2 = df[df.F6 == 2]

    # Данные для диаграммы
    y1 = dg1.Duration.value_counts().sort_index()[:180]
    y2 = dg2.Duration.value_counts().sort_index()[:180]
    x = range(180)

    fig, ax = plt.subplots()
    fig.set_figwidth(16)    #  ширина и
    fig.set_figheight(12)   #  высота диаграммы

    ax.plot(x, y1, linestyle = '-', linewidth = 2.5, color = 'darkblue') 
    ax.plot(x, y2, linestyle = '-', linewidth = 2.5, color = 'darkmagenta')
    ax.set_xlabel('Время обслуживания вызова, мин.', fontsize = '14') # название оси X
    ax.set_ylabel('Частота случаев, ед.', fontsize = '14') # название оси Y
    ax.grid(which='major', color = 'paleturquoise')  # цвет сетки

    x1_max = y1.idxmax()  # индекс максимума
    x2_max = y2.idxmax()  # индекс максимума

    ant1 = f' 1\n({y1.max()} ед.)'
    ant2 = f' 2\n({y2.max()} ед.)'
    
    plt.annotate(ant1, xy=(x1_max, y1[x1_max]),  xycoords='data', xytext=(35, 5000), 
                 textcoords='data', arrowprops=dict(arrowstyle='->'), fontsize = 16, fontweight='bold')
    plt.annotate(ant2, xy=(x2_max, y2[x2_max]),  xycoords='data', xytext=(60, 3000), 
                 textcoords='data', arrowprops=dict(arrowstyle='->'), fontsize = 16, fontweight='bold')

    fig.savefig(f'img/img-014', bbox_inches = 'tight', dpi=300) # сохранить диаграмму в файл
    
    # cnt = {'p27' : x1_max, 'p28' : x2_max, 'p29' : y1.max(), 'p30' : y2.max()}
    
    cnt = {'p27' : fr.Dl(x1_max, 'минута', 1), 'p28' : fr.Dl(x2_max, 'минута', 1), 'p29' : fr.Dl(y1.max(), 'пожар', 1), 'p30' : fr.Dl(y2.max(), 'пожар', 1)}
    return cnt
    

def TypesFS():  # Виды участников тушения пожара
    
    # Запрос - данные для распределения нагрузки по видам пожарной охраны
    strSQL = f'''
    SELECT FR{Yr}.F1, FR{Yr}.F5, FR{Yr}.F6, FR{Yr}.F75, FR{Yr}.F26, FR{Yr}.F61, FR{Yr}.F62, FR{Yr}.F159, FR{Yr}.F160, FR{Yr}.F71, [F43]+[F44] AS S, [F91]+[F92]+[F93] AS Stv, [F83]+[F84]+[F85]+[F86]+[F87] AS T
    FROM FR{Yr}
    WHERE (((FR{Yr}.F75)<>0 AND (FR{Yr}.F75)<>11) AND (([F43]+[F44])>0) AND (([F91]+[F92]+[F93])>0) AND (([F83]+[F84]+[F85]+[F86]+[F87])>0) AND ((FR{Yr}.F4)=0) AND ((Month([F5]))<={Mn}))

    UNION ALL
    SELECT FR{Yr}.F1, FR{Yr}.F5, FR{Yr}.F6, FR{Yr}.F76, FR{Yr}.F26, FR{Yr}.F61, FR{Yr}.F62, FR{Yr}.F159, FR{Yr}.F160, FR{Yr}.F71, [F43]+[F44] AS S, [F91]+[F92]+[F93] AS Stv, [F83]+[F84]+[F85]+[F86]+[F87] AS T
    FROM FR{Yr}
    WHERE (((FR{Yr}.F76)<>0 AND (FR{Yr}.F76)<>11) AND (([F43]+[F44])>0) AND (([F91]+[F92]+[F93])>0) AND (([F83]+[F84]+[F85]+[F86]+[F87])>0) AND ((FR{Yr}.F4)=0) AND ((Month([F5]))<={Mn}))

    UNION ALL
    SELECT FR{Yr}.F1, FR{Yr}.F5, FR{Yr}.F6, FR{Yr}.F77, FR{Yr}.F26, FR{Yr}.F61, FR{Yr}.F62, FR{Yr}.F159, FR{Yr}.F160, FR{Yr}.F71, [F43]+[F44] AS S, [F91]+[F92]+[F93] AS Stv, [F83]+[F84]+[F85]+[F86]+[F87] AS T
    FROM FR{Yr}
    WHERE (((FR{Yr}.F77)<>0 AND (FR{Yr}.F77)<>11) AND (([F43]+[F44])>0) AND (([F91]+[F92]+[F93])>0) AND (([F83]+[F84]+[F85]+[F86]+[F87])>0) AND ((FR{Yr}.F4)=0) AND ((Month([F5]))<={Mn}));
    '''

    openDB()
    df = pd.read_sql(strSQL, conn)
    df =  df.astype(int, errors='ignore')  # Понижение разрядности
    df.F5 = pd.to_datetime(df.F5)  # форматирование полей, содержащих даты
    df.F71 = pd.to_datetime(df.F71)

    # Удаление лишних строк по условию F75
    lst = [4,9]
    df = df[~df.F75.isin(lst)]

    # Продолжительность пожара
    df['Duration'] = ((df.F71 - df.F5).dt.days * 1440 + (df.F159 - df.F61) * 60 + (df.F160 - df.F62))

    # Преобразование типов населенных пунктов: 1 - города и ПГТ; 2 - все остальные
    mask  = df.F6 == 2 # поселки городского типа
    df.loc[mask, 'F6'] = 1  # приравнять к городам
    mask  = df.F6 != 1 # все населенные пункты НЕ города
    df.loc[mask, 'F6'] = 2  # сделать единого типа - 2


    # Сводная таблица: виды ПО - время занятости на пожаре
    n = 1
    df = df.assign(Interval = 0)  # столбец для меток групп сводной таблицы
    for i in range(10, 120, 20):
        mask = (df.Duration > i) & (df.Duration <= (i+20))
        df.loc[mask, 'Interval'] = n
        n += 1

    mask = df.Interval == 0
    df.loc[mask, 'Interval'] = 7
    df_pv1 = pd.pivot_table(df, index=['F75','F6'], columns='Interval', values='Duration', aggfunc=len)
    df_pv1 = del_null(df_pv1)

    # Сводная таблица: виды ПО - количество техники
    n = 1
    df = df.assign(Interval = 0)
    for i in range(0, 12, 2):
        mask = (df['T'] > i) & (df['T'] <= (i+2))    
        df.loc[mask, 'Interval'] = n
        n += 1

    mask = df.Interval == 0
    df.loc[mask, 'Interval'] = 7
    df_pv2 = pd.pivot_table(df, index=['F75','F6'], columns='Interval', values='Duration', aggfunc=len)
    df_pv2 = del_null(df_pv2)

    # Сводная таблица: виды ПО - количество стволов, подаваемых на тушение пожара
    n = 1
    df = df.assign(Interval = 0)
    for i in range(0, 12, 2):
        mask = (df.Stv > i) & (df.Stv <= (i+2))
        df.loc[mask, 'Interval'] = n
        n += 1

    mask = df.Interval == 0
    df.loc[mask, 'Interval'] = 7
    df_pv3 = pd.pivot_table(df, index=['F75','F6'], columns='Interval', values='Duration', aggfunc=len)
    df_pv3 = del_null(df_pv3)

    # Сводная таблица: виды ПО - общая площадь пожара
    n = 1
    df = df.assign(Interval = 0)
    for i in range(0, 600, 100):
        mask = (df.S > i) & (df.S <= (i+100))
        df.loc[mask, 'Interval'] = n
        n += 1

    mask = df.Interval == 0
    df.loc[mask, 'Interval'] = 7
    df_pv4 = pd.pivot_table(df, index=['F75','F6'], columns='Interval', values='Duration', aggfunc=len)
    df_pv4 = del_null(df_pv4)

    
    def Trend(tt):  # Вычисление коэфф. снижения темпа прироста
        weight, S = [1, 0.75, 0.5, 0.25, 0.1], 0
        for i in range(1,6):
            S += (tt[i+1]-tt[i]) / tt[i] * weight[i-1]
        return round(S/5, 2)
    

    # Словарь для таблицы 7  - время обслуживания вызова
    df1 = df_pv1    
    dcs = {} # Основной словарь
    dc, dc1 = {}, {}  # заполнение словаря для таблицы 7
    for i in range(1,8):
        dc1 = {f't7n{n+32*(i-1)}' : j for n, j in enumerate(df1[i])}  # показатели прироста
        dc = {**dc, **dc1}

    Tr = [Trend(df1.iloc[i]) for i in range(df1.shape[0])]
    dc1 = {f't7n{n+224}' : j for n, j in enumerate(Tr)}  # коэфф. прироста

    dc = {**dc, **dc1} 
    dcs = dc.copy()  # неглубокая копия словаря
    
    
    # Словарь для Приложения таблицы  - Сводная таблица: виды ПО - количество техники
    df1 = df_pv2
    
    dc, dc1 = {}, {}  # заполнение словаря для таблицы 7
    for i in range(1,8):
        dc1 = {f'pr14n{n+32*(i-1)}' : j for n, j in enumerate(df1[i])}  # показатели прироста
        dc = {**dc, **dc1}

    Tr = [Trend(df1.iloc[i]) for i in range(df1.shape[0])]
    dc1 = {f'pr14n{n+224}' : j for n, j in enumerate(Tr)}  # коэфф. прироста

    dc = {**dc, **dc1}
    dcs = {**dcs, **dc}

    
    # Словарь для Приложения таблицы  - Сводная таблица: виды ПО -  количество стволов, подаваемых на тушение пожара
    df1 = df_pv3
    
    dc, dc1 = {}, {}  # заполнение словаря для таблицы 7
    for i in range(1,8):
        dc1 = {f'pr15n{n+32*(i-1)}' : j for n, j in enumerate(df1[i])}  # показатели прироста
        dc = {**dc, **dc1}

    Tr = [Trend(df1.iloc[i]) for i in range(df1.shape[0])]
    dc1 = {f'pr15n{n+224}' : j for n, j in enumerate(Tr)}  # коэфф. прироста

    dc = {**dc, **dc1}
    dcs = {**dcs, **dc}
    
    
    # Словарь для Приложения таблицы  - Сводная таблица: виды ПО - общая площадь пожара 
    df1 = df_pv4
    
    dc, dc1 = {}, {}  # заполнение словаря для таблицы 7
    for i in range(1,8):
        dc1 = {f'pr16n{n+32*(i-1)}' : j for n, j in enumerate(df1[i])}  # показатели прироста
        dc = {**dc, **dc1}

    Tr = [Trend(df1.iloc[i]) for i in range(df1.shape[0])]
    dc1 = {f'pr16n{n+224}' : j for n, j in enumerate(Tr)}  # коэфф. прироста

    dc = {**dc, **dc1}
    dcs = {**dcs, **dc} 

    ###########
    dfp = pd.pivot_table(df, index=['F1'], columns='F75', values='F5', aggfunc=len)[:85]
    dfp = dfp.fillna(0)  # замена NaN на 0
    dfp =  dfp.astype(int, errors='ignore')  # понижение разрядности до целых
    dfp.columns = range(1,17)  # новые имена колонок
    dfp['S'] = dfp.sum(axis=1)  # сумма строк

    dfp['Reg'] = ''  # втавить названия регионов
    for i in dfp.index:
        dfp.Reg[i] = fr.RegionName(i, 1)

    dfp = dfp.sort_values(by=['Reg'])
    dfp = dfp.reset_index(drop=True)

    # распределение нагрузки по видам участников тушения пожара (в отчете это виды ПО)
    dfd = dfp.copy()
    for i in range(1,17):
        dfd[i] = round(dfp[i] / dfp.S *100, 1)

    tFS = pd.read_csv('pkl/TypesFS.csv', sep=';')
    S = ''
    for j in range(1, 17):
        vp = dfd.sort_values(by=[j], ascending=False)[:3]
        S += tFS['Types'][j-1] + ':\a'
        for i in vp.index:
            S += f"{vp['Reg'][i]} - {vp[j][i]}%; "
        S = S[:-2] + '.\a'

    cnt1 = {'p34': S}


    # приложение 17
    dc17, dc17t = {}, {}  # заполнение словаря для приложения 17
    for i in range(1,17):
        dc17t = {f'pr17n{n+85*i}' : j for n, j in enumerate(dfp[i])}  # количество пожаров
        dc17 = {**dc17, **dc17t}

    dc17t = {f'pr17n{n}' : j for n, j in enumerate(dfp['Reg'])}
    dc17 = {**dc17, **dc17t} 

    dcs =  {**dcs, **cnt1, **dc17} 
    
    return dcs
    

def Paragraph__17(): # Пожары потушенные только ДПД
    
    df = pd.DataFrame(np.zeros((1, 6), dtype=int), columns=['F1', 'F5', 'F75', 'F76', 'F77', 'ОбъектГр'])
    openDB()
    
    for Yri in range(2010, Yr+1):
        strSQL = f'''
        SELECT FR{Yri}.F1, Year(FR{Yri}.F5) AS F5, FR{Yri}.F75, FR{Yri}.F76, FR{Yri}.F77, т08F12гр_ОбъектыГр.ОбъектГр
        FROM т08F12гр_ОбъектыГр INNER JOIN (т08F12_Объекты INNER JOIN FR{Yri} ON т08F12_Объекты.F12 = FR{Yri}.F12) ON т08F12гр_ОбъектыГр.F12гр = т08F12_Объекты.F12гр
        WHERE (((FR{Yri}.F75)=6) AND ((FR{Yri}.F76)=0) AND ((FR{Yri}.F77)=0) AND ((FR{Yri}.F4)=0) AND ((Month([F5]))<={Mn}))

        UNION ALL
        SELECT FR{Yri}.F1, Year(FR{Yri}.F5) AS F5, FR{Yri}.F75, FR{Yri}.F76, FR{Yri}.F77, т08F12гр_ОбъектыГр.ОбъектГр
        FROM т08F12гр_ОбъектыГр INNER JOIN (т08F12_Объекты INNER JOIN FR{Yri} ON т08F12_Объекты.F12 = FR{Yri}.F12) ON т08F12гр_ОбъектыГр.F12гр = т08F12_Объекты.F12гр
        WHERE (((FR{Yri}.F75)=0) AND ((FR{Yri}.F76)=6) AND ((FR{Yri}.F77)=0) AND ((FR{Yri}.F4)=0) AND ((Month([F5]))<={Mn}))


        UNION ALL
        SELECT FR{Yri}.F1, Year(FR{Yri}.F5) AS F5, FR{Yri}.F75, FR{Yri}.F76, FR{Yri}.F77, т08F12гр_ОбъектыГр.ОбъектГр
        FROM т08F12гр_ОбъектыГр INNER JOIN (т08F12_Объекты INNER JOIN FR{Yri} ON т08F12_Объекты.F12 = FR{Yri}.F12) ON т08F12гр_ОбъектыГр.F12гр = т08F12_Объекты.F12гр
        WHERE (((FR{Yri}.F75)=0) AND ((FR{Yri}.F76)=0) AND ((FR{Yri}.F77)=6) AND ((FR{Yri}.F4)=0) AND ((Month([F5]))<={Mn}))
        ;'''
        dfi = pd.read_sql(strSQL, conn)
        df = df.append(dfi)


    df = df.drop([0])  # удаление первой строки (там были только нули)
    df = df.fillna(0)  # замена NaN на 0
    df =  df.astype(int, errors='ignore')  # понижение разрядности до целых
    df = df.reset_index(drop=True) # обновить нумерацию индексов

    # Сводная таблица
    dfp = pd.pivot_table(df, index='F1', columns='F5', values='F75', fill_value=0, margins=True, aggfunc=len)
    del dfp['All']  # удаление столбца


    dfg = dfp[-1:].T  #  транспонирование строки в колонку
    sns.set()  # стиль seaborn
    fig, ax = plt.subplots(figsize=(10, 6))
    # построить ломаную линию
    sns.lineplot(data = dfg, x='F5', y='All', color='blue', linewidth = 3, ci=None, marker='o', markersize=10)
    for i in dfg.index:   # подписи значений
        plt.text(i-0.08, dfg.All[i]+75, dfg.All[i])

    ax.set_title('ДПД')
    ax.set_ylabel('Количество пожаров, ед.')  #  подписи осей
    ax.set_xlabel('Годы')

    fig.savefig('img/img-015', dpi=300, bbox_inches = 'tight')

    t = dfp[Yr]['All']
    p35 = f'{fr.Dl(t, "ликвидирован")} {fr.Dl(t, "пожар", 1)}'

    t1 = fr.RegionSuperior(dfp, Yr, 4)
    t2 = int(t1.split()[-1][:-1])
    p36 = f'{t1[:t1.rfind("-")]} - {fr.Dl(t2, "пожар", 1)}.'

    t1 = fr.RegionSuperior(dfp, Yr-1, 4)
    t2 = int(t1.split()[-1][:-1])
    p37 = f'{t1[:t1.rfind("-")]} - {fr.Dl(t2, "пожар", 1)}.'

    dfo = df.groupby('ОбъектГр').count()[['F5']].sort_values('F5', ascending=False) # количество пожаров по регионам
    p38 = f'{dfo.index[0].lower()} - {dfo.iloc[0][0]} ед. и {dfo.index[1].lower()} - {dfo.iloc[1][0]} ед.'

    print('Chart__15:', time.strftime("%H:%M:%S", time.localtime()))

    cnt = {'p35' : p35, 'p36' : p36, 'p37' : p37, 'p38' : p38}

    return cnt

