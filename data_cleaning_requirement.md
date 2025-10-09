# Health Fitness Data Cleaning Features Documentation

## 📋 Overview

This document outlines the comprehensive data cleaning features implemented for the health fitness tracking dataset (`health_fitness_tracking_365days.csv`). The cleaning process ensures data quality, removes outliers, and creates additional useful features for analysis.

## 🎯 Dataset Information

- **Original Size**: 365,001 rows (including header)
- **Users**: 1,000 unique users (IDs 0-999)
- **Time Period**: September 6, 2024 to September 5, 2025
- **Columns**: 12 original columns + 8 new derived features

### Original Columns
| Column | Type | Description | Range/Values |
|--------|------|-------------|--------------|
| `user_id` | Integer | Unique user identifier | 0-999 |
| `age` | Integer | User's age in years | 18-100 |
| `gender` | Categorical | User's gender | F, M |
| `date` | Date | Recording date | 2024-09-06 to 2025-09-05 |
| `steps` | Numeric | Daily step count | 0-50,000 |
| `heart_rate_avg` | Numeric | Average heart rate (bpm) | 30-200 |
| `sleep_hours` | Numeric | Hours of sleep | 2-16 |
| `calories_burned` | Numeric | Daily calories burned | 500-8,000 |
| `exercise_minutes` | Numeric | Minutes of exercise | 0-600 |
| `stress_level` | Numeric | Stress level (1-10 scale) | 1-10 |
| `weight_kg` | Numeric | Weight in kilograms | 30-200 |
| `bmi` | Numeric | Body Mass Index | 10-60 |

## 🧹 Data Cleaning Features

### 1. Data Validation & Type Conversion

#### 1.1 Missing Value Detection
- **Feature**: Automatically detects missing values across all columns
- **Action**: Reports missing values and their counts
- **Output**: Summary of missing data per column

#### 1.2 Duplicate Detection
- **Feature**: Identifies and removes duplicate rows
- **Action**: Compares all columns for exact matches
- **Output**: Count of duplicates removed

#### 1.3 Data Type Conversion
- **Date Conversion**: Converts date strings to datetime objects
- **Numeric Conversion**: Ensures all numeric columns are properly typed
- **Categorical Conversion**: Converts gender to categorical type
- **Error Handling**: Uses `errors='coerce'` for safe conversion

### 2. Outlier Detection & Removal

#### 2.1 Health Metric Validation
The system applies domain-specific validation rules for health metrics:

| Metric | Min Value | Max Value | Rationale |
|--------|-----------|-----------|-----------|
| Age | 18 | 100 | Reasonable adult age range |
| Steps | 0 | 50,000 | Extreme daily step limit |
| Heart Rate | 30 | 200 | Resting to maximum heart rate |
| Sleep Hours | 2 | 16 | Minimum viable to maximum possible sleep |
| Calories | 500 | 8,000 | Basal metabolic rate to extreme activity |
| Exercise | 0 | 600 | No exercise to 10 hours daily |
| Stress Level | 1 | 10 | Defined scale range |
| Weight | 30 | 200 | Reasonable weight range in kg |
| BMI | 10 | 60 | Underweight to severely obese |

#### 2.2 Outlier Removal Process
- **Method**: Range-based filtering
- **Action**: Removes rows outside defined ranges
- **Reporting**: Detailed count of outliers removed per column
- **Preservation**: Maintains data integrity while removing unrealistic values

### 3. Data Quality Improvements

#### 3.1 Negative Value Handling
- **Detection**: Identifies negative values in non-negative metrics
- **Action**: Sets negative values to 0
- **Affected Columns**: steps, heart_rate_avg, sleep_hours, calories_burned, exercise_minutes, weight_kg, bmi

#### 3.2 Zero Value Analysis
- **Zero Steps**: Identifies days with zero steps (potential data quality issue)
- **Zero Sleep**: Identifies days with zero sleep hours (impossible)
- **Reporting**: Counts and reports suspicious zero values

### 4. Feature Engineering

#### 4.1 Date-Based Features
| Feature | Description | Values |
|---------|-------------|--------|
| `year` | Year extracted from date | 2024, 2025 |
| `month` | Month number | 1-12 |
| `day_of_week` | Day name | Monday-Sunday |
| `day_of_week_num` | Day number | 0-6 (Monday=0) |
| `is_weekend` | Weekend indicator | True/False |

#### 4.2 Activity Level Classification
Based on daily step count:
- **Sedentary**: 0-4,999 steps
- **Light**: 5,000-9,999 steps
- **Moderate**: 10,000-14,999 steps
- **Active**: 15,000+ steps

#### 4.3 Sleep Quality Classification
Based on sleep hours:
- **Poor**: 0-6 hours
- **Fair**: 6-8 hours
- **Good**: 8-10 hours
- **Excellent**: 10+ hours

#### 4.4 BMI Category Classification
Based on BMI values:
- **Underweight**: BMI < 18.5
- **Normal**: BMI 18.5-24.9
- **Overweight**: BMI 25-29.9
- **Obese**: BMI ≥ 30

## 📊 Data Quality Reporting

### 5.1 Statistical Summary
- **Descriptive Statistics**: Mean, median, standard deviation, min, max for all numeric columns
- **Data Completeness**: Percentage of original data retained after cleaning
- **Shape Comparison**: Before and after cleaning row counts

### 5.2 Distribution Analysis
- **Gender Distribution**: Count and percentage by gender
- **Age Distribution**: Min, max, mean, median age
- **Activity Level Distribution**: Count by activity category
- **Sleep Quality Distribution**: Count by sleep quality category
- **BMI Category Distribution**: Count by BMI category

### 5.3 Quality Metrics
- **Outlier Removal Rate**: Percentage of data removed as outliers
- **Data Completeness**: Percentage of original data retained
- **Quality Issues**: Summary of identified data quality problems


## 📈 Expected Outputs

### 1. Console Output
- Real-time progress updates with emojis
- Detailed cleaning statistics
- Data quality report
- Feature engineering summary

### 2. Cleaned Dataset
- CSV file with cleaned data
- Additional derived features
- Consistent data types
- Outlier-free data

### 3. Data Quality Report
- Comprehensive statistics
- Distribution analysis
- Quality metrics summary

## 🔧 Customization Options

### Modifying Outlier Ranges
```python
outlier_rules = {
    'age': (18, 100),           # Adjust age range
    'steps': (0, 50000),        # Adjust step limits
    'heart_rate_avg': (30, 200), # Adjust heart rate range
    # ... modify other ranges as needed
}
```

### Adding New Features
```python
# Add custom features in the feature engineering section
df_clean['custom_feature'] = df_clean['column1'] / df_clean['column2']
```

### Modifying Categories
```python
# Adjust activity level bins
df_clean['activity_level'] = pd.cut(df_clean['steps'], 
                                   bins=[0, 3000, 7000, 12000, float('inf')], 
                                   labels=['Very Low', 'Low', 'Moderate', 'High'])
```

## 🎯 Benefits

1. **Data Quality**: Ensures reliable, clean data for analysis
2. **Outlier Removal**: Eliminates unrealistic values that could skew results
3. **Feature Enhancement**: Adds meaningful derived features
4. **Automated Process**: Reduces manual data cleaning effort
5. **Comprehensive Reporting**: Provides detailed insights into data quality
6. **Reproducible**: Consistent cleaning process across different datasets
7. **Extensible**: Easy to modify and extend for different use cases

## 📝 Notes

- The cleaning process is designed specifically for health and fitness data
- Outlier ranges are based on medical and fitness industry standards
- All changes are logged and reported for transparency
- The process preserves the original data structure while improving quality
- Additional features are added without modifying original columns

## 🔄 Version History

- **v1.0**: Initial implementation with basic cleaning features
- **v1.1**: Added comprehensive outlier detection
- **v1.2**: Enhanced feature engineering capabilities
- **v1.3**: Improved data quality reporting

---

*This documentation is automatically generated and should be updated when new cleaning features are added.*