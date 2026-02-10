# Data Sources Documentation

## Primary Data Source: Kaggle Amazon Product Dataset

### Dataset Details:
- **Source**: Kaggle - Amazon Product Dataset 2020
- **Original Size**: 10,000+ products across multiple categories
- **Collection Method**: Web scraping of Amazon.com (publicly available data)
- **License**: CC0: Public Domain
- **URL**: https://www.kaggle.com/datasets/PromptCloudHQ/amazon-product-dataset-2020

### Data Fields Used:
1. **product_name**: Name/title of the product
2. **brand**: Manufacturer or brand name
3. **category**: Product category/subcategory
4. **price**: Retail price in USD
5. **sale_price**: Discounted price (if available)
6. **rating**: Customer rating (1-5 stars)
7. **review_count**: Number of customer reviews
8. **description**: Product description text
9. **url**: Product page URL

### Eco-Product Filtering Methodology:

#### Keywords Used for Filtering:
#### Processing Steps:
1. **Text Analysis**: Combined product name, brand, and description
2. **Keyword Matching**: Identified products containing eco-related terms
3. **Category Enrichment**: Added eco-specific categories and attributes
4. **Data Enhancement**: Added calculated fields (discount %, price categories)

### Ethical Considerations:
- Using publicly available dataset with proper attribution
- No active scraping of live websites
- All data is anonymized and aggregated
- Compliant with Kaggle's terms of service

### Dataset Statistics:
- **Total Products Processed**: [AUTO-FILLED]
- **Eco-Friendly Products Identified**: [AUTO-FILLED]
- **Coverage**: Home, Kitchen, Cleaning, Personal Care categories
- **Time Period**: 2020 product listings

### Quality Assurance:
1. **Price Validation**: Removed outliers and invalid prices
2. **Duplicate Removal**: Eliminated identical product entries
3. **Missing Data Handling**: Imputed or flagged incomplete records
4. **Consistency Checks**: Ensured price > sale_price where applicable

### Supplementary Data:
- **Synthetic Data Generation**: Created realistic eco-products for demonstration
- **Brand Categorization**: Classified brands as established/emerging/eco-focused
- **Attribute Extraction**: Parsed sustainability features from descriptions

### Usage Notes:
This dataset is suitable for:
- Market analysis of eco-friendly products
- Competitor pricing intelligence
- Sustainability trend analysis
- Product recommendation systems

### Update Frequency:
- Base dataset: Static (2020 snapshot)
- Attributes and enhancements: Updated during analysis phase
