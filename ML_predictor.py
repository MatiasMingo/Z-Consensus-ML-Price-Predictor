import numpy as np
import pandas as pd
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import MinMaxScaler
from tensorflow.keras.models import Sequential
from tensorflow.keras.layers import LSTM, Dense
from keras.models import load_model



def predict_LSTM(x_1D, x_1W, x_1M):
    non_numerical_columns = ['Ticker']
    
    # Save the non-numerical columns
    non_numerical_1D = x_1D[non_numerical_columns]
    non_numerical_1W = x_1W[non_numerical_columns]
    non_numerical_1M = x_1M[non_numerical_columns]
    
    # Drop non-numerical columns from input DataFrames
    x_1D.drop(columns=non_numerical_columns, inplace=True)
    x_1W.drop(columns=non_numerical_columns, inplace=True)
    x_1M.drop(columns=non_numerical_columns, inplace=True)

    x_1D = x_1D.values.reshape(x_1D.shape[0], 1, x_1D.shape[1])
    x_1W = x_1W.values.reshape(x_1W.shape[0], 1, x_1W.shape[1])
    x_1M = x_1M.values.reshape(x_1M.shape[0], 1, x_1M.shape[1])
    
    model_1D = load_model("models/LSTM_1D.h5")
    model_1W = load_model("models/LSTM_1W.h5")
    model_1M = load_model("models/LSTM_1M.h5")
    
    predictions_1D = model_1D.predict(x_1D)
    predictions_1W = model_1W.predict(x_1W)
    predictions_1M = model_1M.predict(x_1M)
    
    # Create DataFrames for predictions
    df_1D = pd.DataFrame(predictions_1D, columns=['Prediction_1D'])
    df_1W = pd.DataFrame(predictions_1W, columns=['Prediction_1W'])
    df_1M = pd.DataFrame(predictions_1M, columns=['Prediction_1M'])
    
    # Concatenate predictions and non-numerical columns
    result_1D = pd.concat([non_numerical_1D, df_1D], axis=1)
    result_1W = pd.concat([non_numerical_1W, df_1W], axis=1)
    result_1M = pd.concat([non_numerical_1M, df_1M], axis=1)

    return result_1D, result_1W, result_1M

