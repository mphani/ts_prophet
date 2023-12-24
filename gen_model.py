import pandas as pd

G_LOCAL_DATA_FOLDER="metrics_data"

def load_csv(p_filename: str):
    l_filename = p_filename
    
    if not l_filename.startswith(G_LOCAL_DATA_FOLDER):
        l_filename= G_LOCAL_DATA_FOLDER+"/"+ l_filename
    
    return pd.read_csv( l_filename)

summary = pd.DataFrame(data.dtypes, columns=['dtype'])
summary = summary.reset_index()
summary = summary.rename(columns={'index':'Name'})
summary['Null_Counts'] = data.isnull().sum().values
summary['Uniques'] = data.nunique().values
summary['Null_Percent'] = (summary['Null_Counts']*100) / len(data)
summary.sort_values(by='Null_Percent', ascending=False, inplace=True)
summary.info()
data['Title'].unique()
print(X_train.Embarked.mode()) # Most frequent category

# unique
X['FUEL'].value_counts()
# null checks
[var for var in X_train.columns if X_train[var].isnull().sum() > 0]

X_train, X_test, y_train, y_test = train_test_split(data.drop('SalePrice', axis=1), # predictors
                                                    data.SalePrice, # target
                                                    test_size=0.1,
                                                    random_state=0)  # for reproducibility
X_train.shape, X_test.shape, y_train.shape, y_test.shape

# Imputation
# Imputate numerical variables
imputer = SimpleImputer(strategy='constant', fill_value=-1)
X_train['LotFrontage'] = imputer.fit_transform(X_train['LotFrontage'].to_frame())
X_test['LotFrontage'] = imputer.transform(X_test['LotFrontage'].to_frame())

imputer = SimpleImputer(strategy='most_frequent')
X_train[vars_num] = imputer.fit_transform(X_train[vars_num])
X_test[vars_num] = imputer.transform(X_test[vars_num])

if __name__ == "__main__":
    print("Start: gen_model")
    pd = load_csv( "18122023_aerospike_stats.csv")