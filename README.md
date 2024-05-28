# Real Estate Evaluator

![Untitled](https://github.com/gunba/real-estate-evaluator/assets/11908184/0fbd2cbe-9aa0-4713-ae83-f5dd53da4919)

This repository contains tools to predict real estate values in Perth using machine learning regression. By merging data from various sources, the project aims to identify undervalued properties by comparing predicted prices against realtor prices.

## Features

### Data Collection
- **ABS Data**: Census and demographic information.
- **Content Data**: Additional contextual data.
- **Mesh Data**: Spatial data for properties.
- **OSM Data**: OpenStreetMap data for geographic features.
- **REIWA Data**: Real estate information.
- **SCSA Data**: School and education data.
- **WAPOL Data**: Crime statistics from WA Police.

### Data Processing
- **Data Merging**: Integrates data from all sources into a unified dataset.
- **Feature Engineering**: Prepares and processes features for modeling.

### Model Training
- **XGBoost Model**: Trains a regression model to predict property prices.

## Usage

1. **Data Preparation**: Run `build_suburb_data.py` and `build_property_data.py` to prepare datasets.
2. **Model Training**: Use `model_implementation.ipynb` to train the XGBoost model.
3. **Prediction and Evaluation**: Compare model predictions with realtor prices to find potential deals.

## Try It Yourself

See the PowerBI file `real_estate_modelling.pbix` for an interactive exploration of the data and model results.

