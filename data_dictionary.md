# DATA DICTIONARY: Cleaned Eco-Friendly Product Dataset

## Overview
This document describes each column in the cleaned dataset after processing through the data cleaning pipeline.

## Column Descriptions

### brand
- **Description**: Manufacturer or brand name
- **Data Type**: string
- **Cleaning Applied**: Standardized brand names, title case
- **Example**: Public Goods

### category
- **Description**: Product category (standardized to 8 categories)
- **Data Type**: string
- **Cleaning Applied**: Inferred from product name, mapped to standard categories
- **Possible Values**: Kitchen, Cleaning, Bath & Personal Care, Laundry, Home & Garden, Reusable Items, Bamboo Products, Other

### price
- **Description**: Original retail price in USD
- **Data Type**: float64
- **Cleaning Applied**: Validated range ($0.01-$1000), outliers capped
- **Value Range**: $5.43 - $39.98

### sale_price
- **Description**: Current sale/discounted price in USD
- **Data Type**: float64
- **Cleaning Applied**: Set equal to price if no sale, validated range
- **Note**: Equals price when not on sale

### rating
- **Description**: Customer rating on a 1-5 scale
- **Data Type**: float64
- **Cleaning Applied**: Clipped to 1-5 range, missing filled with category mean
- **Value Range**: 1.0 - 5.0

### review_count
- **Description**: Number of customer reviews
- **Data Type**: int64
- **Cleaning Applied**: Missing values filled with 0
- **Value Range**: 0 - 494

### description
- **Description**: Product description text
- **Data Type**: string
- **Cleaning Applied**: Text cleaned, encoding fixed
- **Note**: May be truncated in some cases

### website
- **Description**: Source website where product was found
- **Data Type**: string
- **Cleaning Applied**: Standardized to major retailers
- **Possible Values**: Amazon, Package Free Shop, EarthHero, Brand Website, Etsy, Walmart, Other Retailer

### date_collected
- **Description**: Date when data was collected
- **Data Type**: string
- **Note**: Collection date from original dataset

### attributes
- **Description**: Original sustainability attributes text
- **Data Type**: string
- **Cleaning Applied**: Split and standardized in attributes_cleaned

### brand_type
- **Description**: Derived or auxiliary column
- **Data Type**: str
- **Unique Values**: 4

### on_sale
- **Description**: Flag indicating if product is currently on sale
- **Data Type**: bool

### discount_pct
- **Description**: Percentage discount from original price
- **Data Type**: float64
- **Value Range**: 0.0 - 100.0
- **Note**: 0 when not on sale

### price_category
- **Description**: Derived or auxiliary column
- **Data Type**: str
- **Unique Values**: 3

### product_name
- **Description**: Name/title of the eco-friendly product
- **Data Type**: string
- **Cleaning Applied**: Cleaned text, title case, special characters removed
- **Example**: Bamboo Toothbrush Set - 4 Pack

### attributes_cleaned
- **Description**: List of standardized sustainability attributes
- **Data Type**: list
- **Cleaning Applied**: Extracted and standardized from attributes/description

### has_sustainable
- **Description**: Derived or auxiliary column
- **Data Type**: bool
- **Unique Values**: 1

### has_eco_friendly
- **Description**: Derived or auxiliary column
- **Data Type**: bool
- **Unique Values**: 1

### price_ratio
- **Description**: Derived or auxiliary column
- **Data Type**: float64
- **Unique Values**: 60

### price_tier
- **Description**: Categorical price grouping
- **Data Type**: string

### review_score
- **Description**: Bayesian average rating weighted by review count
- **Data Type**: float64
- **Value Range**: 1.0 - 5.0

### has_credible_reviews
- **Description**: Flag for products with sufficient reviews (≥10)
- **Data Type**: bool

### name_length
- **Description**: Derived or auxiliary column
- **Data Type**: int64
- **Unique Values**: 1

### desc_length
- **Description**: Derived or auxiliary column
- **Data Type**: int64
- **Unique Values**: 3

### has_description
- **Description**: Derived or auxiliary column
- **Data Type**: bool
- **Unique Values**: 1

### brand_category
- **Description**: Classification of brand sustainability focus
- **Data Type**: string


## Data Quality Notes

### Completeness
- No missing values in critical columns (product_name, brand, category, price)
- Minimal missing values in other columns (imputed during cleaning)

### Consistency
- All prices in USD
- Ratings standardized to 1-5 scale
- Categories mapped to standardized set

### Validity
- Prices within reasonable bounds ($0.01 - $1000)
- Ratings clipped to valid range (1-5)
- Review counts are non-negative integers

### Derived Columns
Columns prefixed with 'has_' (e.g., has_bamboo) are boolean flags indicating presence of sustainability attributes.

## Usage Guidelines

1. **Primary Analysis Columns**: product_name, brand, category, price, rating, review_count
2. **Pricing Analysis**: price, sale_price, discount_pct, price_tier, on_sale
3. **Sustainability Analysis**: attributes_cleaned, has_* flags, brand_category
4. **Quality Filtering**: has_credible_reviews, review_score

## Contact
For questions about this dataset, refer to the cleaning summary report or project documentation.
