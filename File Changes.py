import pandas as pd
import os
import re
import warnings
warnings.filterwarnings("ignore", "This pattern is interpreted as a regular expression")
warnings.filterwarnings("ignore", "This pattern has match groups. To actually get the groups, use str.extract.")

#Text Format Function
def lowAndStrip(text):
    text = str(text)
    text = text.lower().strip()
    text = " ".join(text.split())
    return text


#Selection of files and reading keyword file
files = list(filter( lambda x:x.endswith((".csv",".xlsx")),os.listdir()))
print(f"Select your files\n{17*'*'}")
for nu,file in enumerate(files):
    print(f"{nu}. {file}")

df = files[int(input("\nSelect sentiment dedup : "))]
print(f"Sentiment dedup = {df}\n")

kw_df = files[int(input("Select keyword file : "))]
print(f"Keyword file = {kw_df}\n")


kw_df = pd.read_excel(kw_df)
kw_df.fillna("",inplace=True)
first_move_index = list(kw_df.columns).index(list(filter(lambda x:"Move To" in x,kw_df.columns))[0])
filter_columns = kw_df.columns[1:first_move_index]

#Formating keyword filter
for col in kw_df.columns:
    kw_df[col] = kw_df[col].apply(lowAndStrip)


#Regex Maker Functions
def notCondition(text):
    need_words = re.findall("\#\s*(\w+)*", text)
    need_words = "|".join([x for x in need_words if len(x) > 1])
    #     need_words
    dont_need_words = re.findall("!\s*(\w+)*", text)
    dont_need_words = "|".join([x for x in dont_need_words if len(x) > 1])
    #     dont_need_words
    if need_words == "":
        regex_text = f"^(?!.*({dont_need_words}))"
    else:
        regex_text = f"^(?!.*({dont_need_words})).*({need_words}).*"
    return regex_text

def andCondition(word):
    base = r"^{}"
    expr = "(?=.*{})"
    words = word.split("&")
    words = [lowAndStrip(x) for x in words]
    return base.format("".join(expr.format(w) for w in words))



#Make the filter df

def filterSymbol(kw):
    if any(map(kw.__contains__, ["|","!","#","&","*"])):
        symbol = ".str.contains"
    elif kw.startswith(('>=','<=','!=','==')):
        symbol = kw[:2]
    elif kw.startswith(('>','<')):
        symbol = kw[0]
    else:
        symbol = '=='
    return symbol

def decideNumber(text):
    try:
        return int(text)
    except:
        try:
            return float(text)
        except:
            return False

def makeFilter(col,kw):
    filters = []
    for k in list(filter (lambda x:len(x)>0,kw.split(","))): 
        k = k.strip()
        symbol = filterSymbol(k) 
        if "&" in k:
                k = andCondition(k)
        elif "!" in k:
            k = notCondition(k)
            
        if k.endswith('"') or k.startswith('"'):
            quote = "'"
        elif symbol != ".str.contains" and decideNumber(k[len(symbol):]):
            k = decideNumber(k[len(symbol):])
            quote = ""
        else:
            quote = '"'
            
        if  symbol == ".str.contains":
            filters.append( f"""(df['{col}']{symbol}({quote}{k}{quote}))""" )
        else:
            filters.append( f"""(df['{col}']{symbol}{quote}{k}{quote})""")
    return f'({"|".join(filters)})' if len(filters) > 1 else filters[0]


def getFilteredDf(df,row):
    filters = row[1:first_move_index]
    active_filters = list(filter(lambda x:x[1] != "",zip(filter_columns,filters)))
    filtered_df = df[eval("&".join([makeFilter(item[0],item[1]) for item in active_filters]))].copy()
    if row[0] == 'keep':
        return filtered_df,active_filters
    else:
        return filtered_df



#Every action 
def removeAction(df,row):
    temp_df = getFilteredDf(df,row)
    df.drop(temp_df.index,inplace=True)
    row['Rows Changed']=temp_df.shape[0]
    return df,row

def moveAction(df,row):
    temp_df = getFilteredDf(df,row)
    df.drop(temp_df.index,inplace=True)
    move_actions = row[first_move_index:]
    move_columns = [x[8:] for  x in kw_df.columns[first_move_index:]] #8 is the length of string 'Move To '
    active_move_actions = list(filter(lambda x:x[1] != "",zip(move_columns,move_actions)))
    for item in active_move_actions:
        temp_df[item[0]] = item[1]
    row['Rows Changed']=temp_df.shape[0]
    return pd.concat([df,temp_df]), row

def keepAction(df,row):
    row1 = row.copy()
    temp_df,active_filters = getFilteredDf(df,row1)
    df.drop(temp_df.index,inplace=True)
    row1[row1[active_filters[-1][0]]] = ""
    big_temp,_ = getFilteredDf(df,row1)
    df.drop(big_temp.index,inplace=True)
    row['Rows Changed']=temp_df.shape[0]
    return pd.concat([df,temp_df]),row

#keyword file checks


#Reading df
print(f'Reading sentiment dedup')
df = pd.read_csv(df,low_memory=False)
df['Og Sentence'] = df['Sentence'].copy()


for col in filter_columns:
    if df.dtypes[col] == 'O':
        df[col] = df[col].apply(lowAndStrip)

logs = []

if kw_df.shape[0] > 0:
    for ind,row in kw_df.iterrows():
        if row['Action'] == 'remove':
            df,log = removeAction(df,row)
        elif row['Action'] == 'move':
            df,log = moveAction(df,row)
        elif row['Action'] == 'keep':
            df,log = keepAction(df,row)
        
        if log.shape[0] > 0:
            log = log.to_list()
            log.insert(1,log.pop(-1))
            logs.append(log)
            print(log)

df['Sentence'] = df['Og Sentence']
df.drop(columns=['Og Sentence'],inplace=True)
df.to_csv("Cleaned df.csv",index=False)
pd.DataFrame(logs,columns=kw_df.columns.insert(1,"Rows Changed")).to_csv("log.csv",index=False)

print("Saved df and log")
    





