# 3_analysis_insights.py
"""
ECO-FRIENDLY MARKET INTELLIGENCE - ANALYSIS & INSIGHT GENERATION
Complete analysis pipeline for pricing intelligence, competitor analysis, and market trends
"""

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from datetime import datetime
import warnings
warnings.filterwarnings('ignore')

# Set visualization style
plt.style.use('seaborn-v0_8-darkgrid')
sns.set_palette("husl")
plt.rcParams['figure.figsize'] = (12, 6)
plt.rcParams['font.size'] = 12

class MarketIntelligenceAnalyzer:
    """Complete analysis pipeline for eco-friendly market intelligence"""

    def __init__(self, data_file='clean_master_dataset.csv'):
        self.data_file = data_file
        self.df = None
        self.analysis_results = {}
        self.insights = {}

    def run_analysis_pipeline(self):
        """Execute complete cleaning pipeline"""
        print("="*80)
        print("ECO-FRIENDLY MARKET INTELLIGENCE - ANALYSIS PIPELINE")
        print("="*80)

        # Step 1: Load and Prepare Data
        self.load_and_prepare_data()

        # Step 2: Pricing Intelligence Analysis
        self.analyze_pricing_intelligence()

        # Step 3: Competitor Landscape Analysis
        self.analyze_competitor_landscape()

        # Step 4: Market Trends Analysis
        self.analyze_market_trends()

        # Step 5: Generate Key Insights
        self.generate_key_insights()

        # Step 6: Create Visualizations
        self.create_analysis_visualizations()

        # Step 7: Save Analysis Results
        self.save_analysis_results()

        print("\n" + "="*80)
        print("✅ ANALYSIS PIPELINE COMPLETE")
        print("="*80)

        return self.analysis_results, self.insights

    def load_and_prepare_data(self):
        """Load cleaned data and prepare for analysis"""
        print("\n📥 STEP 1: LOADING AND PREPARING DATA")
        print("-" * 50)

        try:
            self.df = pd.read_csv(self.data_file)
            print(f"✅ Loaded: {self.data_file}")
            print(f"📊 Dataset: {self.df.shape[0]:,} products, {self.df.shape[1]} columns")

            # Convert date if present
            if 'date_collected' in self.df.columns: # Fixed: Check if 'date_collected' column exists
                self.df['date_collected'] = pd.to_datetime(self.df['date_collected'], errors='coerce')

            # Display basic statistics
            print("\n📈 BASIC STATISTICS:")
            print(f"• Categories: {self.df['category'].nunique()}")
            print(f"• Brands: {self.df['brand'].nunique()}")
            print(f"• Websites: {self.df['website'].nunique()}")
            print(f"• Average Price: ${self.df['price'].mean():.2f}")
            print(f"• Average Rating: {self.df['rating'].mean():.2f}/5")
            print(f"• Products on Sale: {(self.df['on_sale'].mean()*100):.1f}%")

            # Prepare analysis-ready data
            self._prepare_analysis_data()

        except Exception as e:
            print(f"❌ Error loading data: {e}")
            raise

    def _prepare_analysis_data(self):
        """Create analysis-ready derived data"""
        print("\n🔧 Preparing analysis-ready data...")

        # Define key competitors for analysis (based on data)
        if 'brand' in self.df.columns:
            top_brands = self.df['brand'].value_counts().head(10).index.tolist()
            self.df['is_key_competitor'] = self.df['brand'].isin(top_brands)
            print(f"📊 Identified {len(top_brands)} key competitors")

        # Create price premium calculation
        if 'price' in self.df.columns and 'category' in self.df.columns:
            category_avg_price = self.df.groupby('category')['price'].mean()
            self.df['price_premium_pct'] = (
                (self.df['price'] - self.df['category'].map(category_avg_price)) /
                self.df['category'].map(category_avg_price) * 100
            ).round(2)

        # Create product success score (composite metric)
        if all(col in self.df.columns for col in ['rating', 'review_count', 'price']):
            # Normalize metrics
            self.df['rating_norm'] = (self.df['rating'] - 1) / 4  # 1-5 to 0-1
            self.df['review_norm'] = np.log1p(self.df['review_count']) / np.log1p(self.df['review_count'].max())
            self.df['price_norm'] = 1 - (self.df['price'] / self.df['price'].max())  # Lower price = better

            # Weighted composite score
            self.df['success_score'] = (
                0.5 * self.df['rating_norm'] +
                0.3 * self.df['review_norm'] +
                0.2 * self.df['price_norm']
            ).round(3)

            print("✅ Created composite success score")

    def analyze_pricing_intelligence(self):
        """Analyze pricing strategies across the market"""
        print("\n💰 STEP 2: PRICING INTELLIGENCE ANALYSIS")
        print("-" * 50)

        analysis_results = {}

        # 1. Overall Market Pricing
        analysis_results['price_stats'] = {
            'mean': self.df['price'].mean(),
            'median': self.df['price'].median(),
            'std': self.df['price'].std(),
            'min': self.df['price'].min(),
            'max': self.df['price'].max(),
            'q1': self.df['price'].quantile(0.25),
            'q3': self.df['price'].quantile(0.75)
        }

        print(f"📊 Overall Price Statistics:")
        print(f"  • Average: ${analysis_results['price_stats']['mean']:.2f}")
        print(f"  • Median: ${analysis_results['price_stats']['median']:.2f}")
        print(f"  • Range: ${analysis_results['price_stats']['min']:.2f} - ${analysis_results['price_stats']['max']:.2f}")
        print(f"  • Std Dev: ${analysis_results['price_stats']['std']:.2f}")

        # 2. Pricing by Category
        print("\n📁 Pricing by Category:")
        category_price_stats = self.df.groupby('category').agg({
            'price': ['mean', 'median', 'count', 'std'],
            'on_sale': 'mean'
        }).round(2)

        # Flatten multi-index columns
        category_price_stats.columns = ['_'.join(col).strip() for col in category_price_stats.columns.values]
        category_price_stats = category_price_stats.sort_values('price_mean', ascending=False)

        analysis_results['category_pricing'] = category_price_stats

        for idx, row in category_price_stats.iterrows():
            print(f"  • {idx:25} | ${row['price_mean']:6.2f} avg | {row['price_count']:4} products | {row['on_sale_mean']*100:4.1f}% on sale")

        # 3. Price Tier Distribution
        if 'price_tier' in self.df.columns:
            print("\n🏷️ Price Tier Distribution:")
            tier_dist = self.df['price_tier'].value_counts().sort_index()
            analysis_results['price_tier_dist'] = tier_dist

            for tier, count in tier_dist.items():
                pct = (count / len(self.df)) * 100
                print(f"  • {tier:20} | {count:5,} products ({pct:.1f}%)")

        # 4. Discount Analysis
        if 'discount_pct' in self.df.columns and 'on_sale' in self.df.columns:
            discount_stats = self.df[self.df['on_sale']]['discount_pct'].describe()
            analysis_results['discount_stats'] = discount_stats

            print(f"\n🎯 Discount Analysis:")
            print(f"  • Products on Sale: {self.df['on_sale'].sum():,} ({(self.df['on_sale'].mean()*100):.1f}%)")
            print(f"  • Average Discount: {discount_stats['mean']:.1f}%")
            print(f"  • Max Discount: {discount_stats['max']:.1f}%")

            # Discounts by category
            discount_by_category = self.df[self.df['on_sale']].groupby('category')['discount_pct'].agg(['mean', 'count']).round(1)
            discount_by_category = discount_by_category.sort_values('mean', ascending=False)
            analysis_results['discount_by_category'] = discount_by_category

            print(f"\n🏷️ Highest Average Discounts by Category:")
            for category, row in discount_by_category.head(5).iterrows():
                print(f"  • {category:25} | {row['mean']:.1f}% avg discount ({row['count']} products)")

        # 5. Price vs Rating Correlation
        if all(col in self.df.columns for col in ['price', 'rating']):
            correlation = self.df[['price', 'rating']].corr().iloc[0, 1]
            analysis_results['price_rating_correlation'] = correlation

            print(f"\n📈 Price-Rating Correlation: {correlation:.3f}")
            if correlation > 0.1:
                print(f"  📊 Insight: Higher prices correlate with higher ratings")
            elif correlation < -0.1:
                print(f"  📊 Insight: Lower prices correlate with higher ratings")
            else:
                print(f"  📊 Insight: Little correlation between price and rating")

        self.analysis_results['pricing_intelligence'] = analysis_results
        print("\n✅ Pricing intelligence analysis complete")

    def analyze_competitor_landscape(self):
        """Analyze competitor positioning and strategies"""
        print("\n🏢 STEP 3: COMPETITOR LANDSCAPE ANALYSIS")
        print("-" * 50)

        analysis_results = {}

        # 1. Market Share by Brand (Product Count)
        brand_market_share = self.df['brand'].value_counts().head(15)
        analysis_results['brand_market_share'] = brand_market_share

        print("📊 Top 15 Brands by Product Count:")
        for i, (brand, count) in enumerate(brand_market_share.items(), 1):
            pct = (count / len(self.df)) * 100
            print(f"  {i:2}. {brand:25} | {count:4} products ({pct:.1f}%)")

        # 2. Brand Performance Analysis
        print("\n⭐ Brand Performance Analysis (Minimum 5 products):")

        # Filter brands with sufficient products
        brand_product_counts = self.df['brand'].value_counts()
        significant_brands = brand_product_counts[brand_product_counts >= 5].index

        brand_performance = self.df[self.df['brand'].isin(significant_brands)].groupby('brand').agg({
            'price': 'mean',
            'rating': 'mean',
            'review_count': 'sum',
            'success_score': 'mean',
            'on_sale': 'mean'
        }).round(2)

        brand_performance['product_count'] = brand_product_counts[significant_brands]
        brand_performance = brand_performance.sort_values('success_score', ascending=False)
        analysis_results['brand_performance'] = brand_performance

        print("\n🏆 Top 10 Brands by Success Score:")
        top_brands = brand_performance.head(10)
        for i, (brand, row) in enumerate(top_brands.iterrows(), 1):
            print(f"  {i:2}. {brand:25} | Score: {row['success_score']:.3f} | "
                  f"${row['price']:.2f} avg | {row['rating']:.1f}★ | {row['product_count']:2} products")

        # 3. Competitive Positioning Matrix
        print("\n🎯 Competitive Positioning Analysis:")

        # Calculate positioning metrics
        positioning_data = []
        for brand in significant_brands:
            brand_data = self.df[self.df['brand'] == brand]
            if len(brand_data) >= 3:  # Only analyze brands with at least 3 products
                positioning_data.append({
                    'brand': brand,
                    'avg_price': brand_data['price'].mean(),
                    'avg_rating': brand_data['rating'].mean(),
                    'market_coverage': len(brand_data),
                    'discount_aggressiveness': brand_data['on_sale'].mean() * 100,
                    'price_premium_avg': brand_data['price_premium_pct'].mean() if 'price_premium_pct' in brand_data.columns else 0
                })

        positioning_df = pd.DataFrame(positioning_data)
        analysis_results['brand_positioning'] = positioning_df

        # Identify strategic positions
        price_threshold = positioning_df['avg_price'].median()
        rating_threshold = positioning_df['avg_rating'].median()

        positioning_df['position'] = positioning_df.apply(
            lambda row: 'Premium & High Quality' if row['avg_price'] > price_threshold and row['avg_rating'] > rating_threshold else
                       'Value & High Quality' if row['avg_price'] <= price_threshold and row['avg_rating'] > rating_threshold else
                       'Premium & Average Quality' if row['avg_price'] > price_threshold and row['avg_rating'] <= rating_threshold else
                       'Value & Average Quality',
            axis=1
        )

        print("\n🏷️ Brand Positioning Categories:")
        position_counts = positioning_df['position'].value_counts()
        for position, count in position_counts.items():
            print(f"  • {position:30} | {count:2} brands")

        # 4. Price Leader Analysis
        print("\n👑 Price Leadership Analysis:")

        # Find price leaders in each category
        if 'category' in self.df.columns:
            category_leaders = {}
            for category in self.df['category'].unique():
                category_data = self.df[self.df['category'] == category]
                if len(category_data) >= 10:  # Only analyze categories with sufficient data
                    # Find brand with highest average price (price leader)
                    brand_prices = category_data.groupby('brand')['price'].mean()
                    if not brand_prices.empty:
                        price_leader = brand_prices.idxmax()
                        leader_price = brand_prices.max()
                        avg_category_price = category_data['price'].mean()
                        premium_pct = ((leader_price - avg_category_price) / avg_category_price) * 100

                        category_leaders[category] = {
                            'price_leader': price_leader,
                            'leader_price': leader_price,
                            'category_avg': avg_category_price,
                            'premium_pct': premium_pct
                        }

            analysis_results['category_price_leaders'] = category_leaders

            print("💰 Price Leaders by Category:")
            for category, data in category_leaders.items():
                print(f"  • {category:25} | {data['price_leader']:20} | "
                      f"${data['leader_price']:.2f} (${data['category_avg']:.2f} avg, +{data['premium_pct']:.1f}%)")

        self.analysis_results['competitor_analysis'] = analysis_results
        print("\n✅ Competitor landscape analysis complete")

    def analyze_market_trends(self):
        """Analyze market trends and consumer preferences"""
        print("\n📈 STEP 4: MARKET TRENDS ANALYSIS")
        print("-" * 50)

        analysis_results = {}

        # 1. Sustainability Attribute Analysis
        print("🌿 Sustainability Attribute Analysis:")

        # Count attribute frequency
        if 'attributes_cleaned' in self.df.columns:
            # Create list of all attributes
            all_attributes = []
            for attrs in self.df['attributes_cleaned'].dropna():
                if isinstance(attrs, str) and attrs != 'nan':
                    # Parse string representation of list
                    try:
                        if attrs.startswith('[') and attrs.endswith(']') or attrs.startswith('(’') or attrs.startswith('("') : # Fixed: check for tuple/string list representations
                            attrs_list = eval(attrs)
                        else:
                            attrs_list = [a.strip() for a in attrs.split(',')]
                        all_attributes.extend([a.strip().lower() for a in attrs_list if a])
                    except: # Fixed: Broad except block can hide issues. For this scenario, continue is OK.
                        continue

            from collections import Counter
            attribute_counts = Counter(all_attributes)
            analysis_results['attribute_frequency'] = attribute_counts

            print("\n🏷️ Top 10 Sustainability Attributes:")
            for attribute, count in attribute_counts.most_common(10):
                pct = (count / len(self.df)) * 100
                print(f"  • {attribute:20} | {count:4} products ({pct:.1f}%)")

        # 2. Price Premium for Sustainability Attributes
        print("\n💰 Price Premium Analysis for Sustainability:")

        # Analyze binary attribute columns
        attribute_columns = [col for col in self.df.columns if col.startswith('has_')]

        premium_analysis = []
        for attr_col in attribute_columns:
            attr_name = attr_col.replace('has_', '')
            if attr_name in ['credible_reviews']:  # Skip non-sustainability attributes
                continue

            # Calculate price premium for products with this attribute
            with_attr = self.df[self.df[attr_col] == True]['price'].mean()
            without_attr = self.df[self.df[attr_col] == False]['price'].mean()

            if pd.notna(with_attr) and pd.notna(without_attr) and without_attr > 0:
                premium_pct = ((with_attr - without_attr) / without_attr) * 100
                product_count = self.df[attr_col].sum()

                premium_analysis.append({
                    'attribute': attr_name,
                    'products_with': product_count,
                    'premium_pct': premium_pct,
                    'avg_price_with': with_attr,
                    'avg_price_without': without_attr
                })

        premium_df = pd.DataFrame(premium_analysis)
        if not premium_df.empty: # Check if premium_df is not empty before sorting
            premium_df = premium_df.sort_values('premium_pct', ascending=False)
        analysis_results['attribute_premiums'] = premium_df

        print("💵 Top Attributes by Price Premium:")
        if not premium_df.empty: # Check again before iterating
            for i, row in premium_df.head(10).iterrows():
                print(f"  • {row['attribute']:20} | +{row['premium_pct']:6.1f}% | "
                      f"${row['avg_price_with']:.2f} vs ${row['avg_price_without']:.2f} | "
                      f"{row['products_with']:3} products")
        else:
            print("  No attribute price premium data to display.")

        # 3. Category Growth Opportunities
        print("\n🚀 Category Growth Opportunity Analysis:")

        if 'category' in self.df.columns and 'rating' in self.df.columns and 'review_count' in self.df.columns:
            category_analysis = self.df.groupby('category').agg({
                'price': ['mean', 'count'],
                'rating': 'mean',
                'review_count': 'sum',
                'on_sale': 'mean'
            }).round(2)

            # Flatten columns
            category_analysis.columns = ['_'.join(col).strip() for col in category_analysis.columns.values]

            # Calculate opportunity score
            # Higher rating + more reviews + lower competition = higher opportunity
            category_analysis['avg_reviews_per_product'] = category_analysis['review_count_sum'] / category_analysis['price_count']
            category_analysis['market_saturation'] = category_analysis['price_count'] / category_analysis['price_count'].sum() * 100
            category_analysis['opportunity_score'] = (
                (category_analysis['rating_mean'] / 5) * 0.4 +  # Higher rating = better
                (np.log1p(category_analysis['avg_reviews_per_product']) /
                 np.log1p(category_analysis['avg_reviews_per_product'].max())) * 0.3 +  # More engagement = better
                (1 - (category_analysis['market_saturation'] / 100)) * 0.3  # Less saturated = better
            ).round(3)

            category_analysis = category_analysis.sort_values('opportunity_score', ascending=False)
            analysis_results['category_opportunities'] = category_analysis

            print("📊 Top Categories by Growth Opportunity:")
            for category, row in category_analysis.head(5).iterrows():
                print(f"  • {category:25} | Opportunity: {row['opportunity_score']:.3f} | "
                      f"{row['price_count']:3} products | {row['rating_mean']:.1f}★ avg | "
                      f"{row['on_sale_mean']*100:.1f}% on sale")

        # 4. Consumer Sentiment Analysis
        print("\n😊 Consumer Sentiment Analysis:")

        if 'rating' in self.df.columns and 'review_count' in self.df.columns:
            # Calculate sentiment distribution
            rating_bins = [0, 2, 3, 4, 5]
            rating_labels = ['Poor (1-2)', 'Average (2-3)', 'Good (3-4)', 'Excellent (4-5)']
            self.df['rating_category'] = pd.cut(self.df['rating'], bins=rating_bins, labels=rating_labels, include_lowest=True)

            sentiment_dist = self.df['rating_category'].value_counts().sort_index()
            analysis_results['sentiment_distribution'] = sentiment_dist

            print("⭐ Rating Distribution:")
            for category, count in sentiment_dist.items():
                pct = (count / len(self.df)) * 100
                print(f"  • {category:15} | {count:5,} products ({pct:.1f}%)")

            # Products with no reviews
            no_reviews = (self.df['review_count'] == 0).sum()
            no_reviews_pct = (no_reviews / len(self.df)) * 100
            print(f"  • No Reviews       | {no_reviews:5,} products ({no_reviews_pct:.1f}%)")

        # 5. Website Performance Analysis
        print("\n🌐 Website Performance Analysis:")

        if 'website' in self.df.columns:
            website_analysis = self.df.groupby('website').agg({
                'price': ['mean', 'count', 'std'],
                'rating': 'mean',
                'on_sale': 'mean',
                'discount_pct': 'mean'
            }).round(2)

            website_analysis.columns = ['_'.join(col).strip() for col in website_analysis.columns.values]
            website_analysis = website_analysis.sort_values('price_count', ascending=False)
            analysis_results['website_performance'] = website_analysis

            print("🏪 Top Websites by Product Count:")
            for website, row in website_analysis.head(5).iterrows():
                print(f"  • {website:20} | {row['price_count']:4} products | "
                      f"${row['price_mean']:.2f} avg | {row['rating_mean']:.1f}★ | "
                      f"{row['on_sale_mean']*100:.1f}% on sale")

        self.analysis_results['market_trends'] = analysis_results
        print("\n✅ Market trends analysis complete")

    def generate_key_insights(self):
        """Generate actionable business insights"""
        print("\n💡 STEP 5: GENERATING KEY INSIGHTS")
        print("-" * 50)

        insights = {}

        # 1. Pricing Strategy Insights
        pricing_data = self.analysis_results.get('pricing_intelligence', {})

        if 'category_pricing' in pricing_data and not pricing_data['category_pricing'].empty:
            # Find most expensive and most discounted categories
            most_expensive = pricing_data['category_pricing'].iloc[0]
            most_discounted = pricing_data.get('discount_by_category', pd.DataFrame()).iloc[0] if 'discount_by_category' in pricing_data and not pricing_data['discount_by_category'].empty else None

            insights['pricing_insights'] = {
                'most_expensive_category': pricing_data['category_pricing'].index[0],
                'most_expensive_avg_price': most_expensive['price_mean'],
                'most_discounted_category': most_discounted.name if most_discounted is not None else 'N/A',
                'avg_discount_rate': pricing_data.get('discount_stats', {}).get('mean', 0) if 'discount_stats' in pricing_data else 0
            }

        # 2. Competitive Landscape Insights
        competitor_data = self.analysis_results.get('competitor_analysis', {})

        if 'brand_performance' in competitor_data and not competitor_data['brand_performance'].empty:
            top_brands = competitor_data['brand_performance'].head(3)
            insights['competitor_insights'] = {
                'top_performing_brands': top_brands.index.tolist(),
                'avg_success_score_top3': top_brands['success_score'].mean(),
                'market_leaders': competitor_data.get('brand_market_share', pd.Series()).head(3).index.tolist() if 'brand_market_share' in competitor_data and not competitor_data['brand_market_share'].empty else []
            }

        # 3. Market Opportunity Insights
        trends_data = self.analysis_results.get('market_trends', {})

        if 'category_opportunities' in trends_data and not trends_data['category_opportunities'].empty:
            top_opportunity = trends_data['category_opportunities'].iloc[0]
            insights['opportunity_insights'] = {
                'highest_opportunity_category': trends_data['category_opportunities'].index[0],
                'opportunity_score': top_opportunity['opportunity_score'],
                'market_saturation': top_opportunity.get('market_saturation', 0)
            }

        # 4. Consumer Preference Insights
        if 'attribute_premiums' in trends_data and not trends_data['attribute_premiums'].empty:
            top_premium_attrs = trends_data['attribute_premiums'].head(3)
            insights['consumer_insights'] = {
                'most_valued_attributes': top_premium_attrs['attribute'].tolist(),
                'highest_premium': top_premium_attrs['premium_pct'].max(),
                'top_attribute': top_premium_attrs.iloc[0]['attribute'] if not top_premium_attrs.empty else 'N/A'
            }

        # 5. Strategic Recommendations
        recommendations = []

        # Recommendation based on pricing
        if 'pricing_insights' in insights:
            rec = f"Focus on {insights['pricing_insights']['most_expensive_category']} category where consumers accept higher prices (${insights['pricing_insights']['most_expensive_avg_price']:.2f} avg)."
            recommendations.append(rec)

        # Recommendation based on attributes
        elif 'consumer_insights' in insights and insights['consumer_insights']['most_valued_attributes']:
            top_attr = insights['consumer_insights']['top_attribute']
            premium = insights['consumer_insights']['highest_premium']
            rec = f"Incorporate '{top_attr}' feature in products - commands {premium:.1f}% price premium."
            recommendations.append(rec)

        # Recommendation based on opportunity
        elif 'opportunity_insights' in insights:
            category = insights['opportunity_insights']['highest_opportunity_category']
            rec = f"Expand into {category} category - high growth opportunity with low market saturation."
            recommendations.append(rec)
        else:
            recommendations.append("No specific strategic recommendations could be generated from available insights.")

        insights['strategic_recommendations'] = recommendations

        # Print insights
        print("\n🎯 KEY BUSINESS INSIGHTS:")
        print("="*60)

        if 'pricing_insights' in insights:
            print(f"\n💰 PRICING STRATEGY:")
            print(f"  • Most expensive category: {insights['pricing_insights']['most_expensive_category']}")
            print(f"  • Average price: ${insights['pricing_insights']['most_expensive_avg_price']:.2f}")
            if insights['pricing_insights']['avg_discount_rate'] > 0:
                print(f"  • Market discount rate: {insights['pricing_insights']['avg_discount_rate']:.1f}%")

        if 'competitor_insights' in insights:
            print(f"\n🏆 COMPETITIVE LANDSCAPE:")
            print(f"  • Top performing brands: {', '.join(insights['competitor_insights']['top_performing_brands'][:3])}")
            print(f"  • Market leaders by volume: {', '.join(insights['competitor_insights']['market_leaders'][:3])}")

        if 'opportunity_insights' in insights:
            print(f"\n🚀 GROWTH OPPORTUNITIES:")
            print(f"  • Highest opportunity category: {insights['opportunity_insights']['highest_opportunity_category']}")
            print(f"  • Opportunity score: {insights['opportunity_insights']['opportunity_score']:.3f}")

        if 'consumer_insights' in insights:
            print(f"\n🌿 CONSUMER PREFERENCES:")
            print(f"  • Most valued attributes: {', '.join(insights['consumer_insights']['most_valued_attributes'][:3])}")
            print(f"  • Highest price premium: {insights['consumer_insights']['highest_premium']:.1f}%")

        print(f"\n🎯 STRATEGIC RECOMMENDATIONS:")
        for i, rec in enumerate(recommendations[:3], 1):
            print(f"  {i}. {rec}")

        self.insights = insights
        print("\n✅ Key insights generated")

    def create_analysis_visualizations(self):
        """Create comprehensive visualizations"""
        print("\n📊 STEP 6: CREATING VISUALIZATIONS")
        print("-" * 50)

        # Create output directory
        import os
        os.makedirs('analysis_visualizations', exist_ok=True)

        # 1. Price Distribution by Category
        plt.figure(figsize=(14, 8))
        category_order = self.df.groupby('category')['price'].median().sort_values(ascending=False).index
        ax = sns.boxplot(data=self.df, x='category', y='price', order=category_order)
        plt.title('Price Distribution by Product Category', fontsize=16, fontweight='bold')
        plt.xlabel('Category', fontsize=12)
        plt.ylabel('Price ($)', fontsize=12)
        plt.xticks(rotation=45, ha='right')
        plt.tight_layout()
        plt.savefig('analysis_visualizations/price_distribution_by_category.png', dpi=300, bbox_inches='tight')
        print("  ✅ Created: Price distribution by category")
        plt.close()

        # 2. Top Brands by Success Score
        if 'success_score' in self.df.columns:
            plt.figure(figsize=(12, 8))
            top_brands = self.df.groupby('brand')['success_score'].mean().nlargest(10).sort_values()

            colors = plt.cm.viridis(np.linspace(0.3, 0.9, len(top_brands)))
            bars = plt.barh(range(len(top_brands)), top_brands.values, color=colors)

            plt.title('Top 10 Brands by Success Score', fontsize=16, fontweight='bold')
            plt.xlabel('Success Score (Composite Metric)', fontsize=12)
            plt.yticks(range(len(top_brands)), top_brands.index)

            # Add value labels
            for bar, value in zip(bars, top_brands.values):
                plt.text(value + 0.01, bar.get_y() + bar.get_height()/2,
                        f'{value:.3f}', ha='left', va='center', fontweight='bold')

            plt.tight_layout()
            plt.savefig('analysis_visualizations/top_brands_success_score.png', dpi=300)
            print("  ✅ Created: Top brands by success score")
            plt.close()

        # 3. Attribute Price Premiums
        trends_data = self.analysis_results.get('market_trends', {})
        if 'attribute_premiums' in trends_data and not trends_data['attribute_premiums'].empty: # Fixed: Added empty check
            premium_df = trends_data['attribute_premiums'].head(8)

            plt.figure(figsize=(12, 6))
            colors = ['green' if x > 0 else 'red' for x in premium_df['premium_pct']]
            bars = plt.bar(range(len(premium_df)), premium_df['premium_pct'], color=colors)

            plt.title('Price Premium for Sustainability Attributes', fontsize=16, fontweight='bold')
            plt.xlabel('Sustainability Attribute', fontsize=12)
            plt.ylabel('Price Premium (%)', fontsize=12)
            plt.xticks(range(len(premium_df)), premium_df['attribute'], rotation=45, ha='right')

            # Add value labels
            for bar, value in zip(bars, premium_df['premium_pct']):
                plt.text(bar.get_x() + bar.get_width()/2, bar.get_height() + (1 if value > 0 else -2),
                        f'{value:.1f}%', ha='center', va='bottom' if value > 0 else 'top',
                        fontweight='bold', fontsize=10)

            plt.axhline(y=0, color='black', linestyle='-', linewidth=0.5)
            plt.tight_layout()
            plt.savefig('analysis_visualizations/attribute_price_premiums.png', dpi=300)
            print("  ✅ Created: Attribute price premiums")
            plt.close()
        else:
            print("  Skipping Attribute Price Premiums visualization: No data or premium_df is empty.") # Fixed: Added else block

        # 4. Category Opportunity Matrix
        if 'category_opportunities' in trends_data and not trends_data['category_opportunities'].empty: # Fixed: Added empty check
            opp_df = trends_data['category_opportunities'].head(6)

            fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(16, 6))

            # Opportunity scores
            ax1.bar(range(len(opp_df)), opp_df['opportunity_score'], color='skyblue')
            ax1.set_title('Growth Opportunity Score by Category', fontsize=14, fontweight='bold')
            ax1.set_xlabel('Category', fontsize=12)
            ax1.set_ylabel('Opportunity Score', fontsize=12)
            ax1.set_xticks(range(len(opp_df)))
            ax1.set_xticklabels(opp_df.index, rotation=45, ha='right')

            for i, value in enumerate(opp_df['opportunity_score']):
                ax1.text(i, value + 0.01, f'{value:.3f}', ha='center', va='bottom', fontweight='bold')

            # Market saturation vs rating
            scatter = ax2.scatter(opp_df['market_saturation'], opp_df['rating_mean'],
                                 s=opp_df['price_count']*10, alpha=0.6, c='coral')
            ax2.set_title('Market Saturation vs Quality (Size = Product Count)', fontsize=14, fontweight='bold')
            ax2.set_xlabel('Market Saturation (%)', fontsize=12)
            ax2.set_ylabel('Average Rating', fontsize=12)

            # Add labels for points
            for i, category in enumerate(opp_df.index):
                ax2.annotate(category[:15],
                            (opp_df.iloc[i]['market_saturation'], opp_df.iloc[i]['rating_mean']),
                            fontsize=9, ha='center')

            plt.tight_layout()
            plt.savefig('analysis_visualizations/category_opportunity_matrix.png', dpi=300)
            print("  ✅ Created: Category opportunity matrix")
            plt.close()
        else:
            print("  Skipping Category Opportunity Matrix visualization: No data or opp_df is empty.") # Fixed: Added else block

        # 5. Competitor Positioning Map
        competitor_data = self.analysis_results.get('competitor_analysis', {})
        if 'brand_positioning' in competitor_data and not competitor_data['brand_positioning'].empty:
            pos_df = competitor_data['brand_positioning']

            plt.figure(figsize=(12, 8))

            # Color by position
            position_colors = {
                'Premium & High Quality': 'green',
                'Value & High Quality': 'blue',
                'Premium & Average Quality': 'orange',
                'Value & Average Quality': 'red'
            }

            colors = [position_colors.get(pos, 'gray') for pos in pos_df['position']]

            scatter = plt.scatter(pos_df['avg_price'], pos_df['avg_rating'],
                                 s=pos_df['market_coverage']*10,
                                 c=colors, alpha=0.7, edgecolors='black')

            plt.title('Competitor Positioning Map', fontsize=16, fontweight='bold')
            plt.xlabel('Average Price ($)', fontsize=12)
            plt.ylabel('Average Rating', fontsize=12)

            # Add brand labels
            for i, row in pos_df.iterrows():
                plt.annotate(row['brand'][:15],
                            (row['avg_price'], row['avg_rating']),
                            fontsize=9, ha='center')

            # Add quadrant lines
            price_median = pos_df['avg_price'].median()
            rating_median = pos_df['avg_rating'].median()
            plt.axhline(y=rating_median, color='gray', linestyle='--', alpha=0.5)
            plt.axvline(x=price_median, color='gray', linestyle='--', alpha=0.5)

            # Add quadrant labels
            plt.text(price_median*1.1, rating_median*1.05, 'Premium & High Quality',
                    fontsize=10, fontweight='bold', color='green')
            plt.text(price_median*0.4, rating_median*1.05, 'Value & High Quality',
                    fontsize=10, fontweight='bold', color='blue')
            plt.text(price_median*1.1, rating_median*0.95, 'Premium & Average Quality',
                    fontsize=10, fontweight='bold', color='orange')
            plt.text(price_median*0.4, rating_median*0.95, 'Value & Average Quality',
                    fontsize=10, fontweight='bold', color='red')

            plt.tight_layout()
            plt.savefig('analysis_visualizations/competitor_positioning_map.png', dpi=300)
            print("  ✅ Created: Competitor positioning map")
            plt.close()
        else:
            print("  Skipping Competitor Positioning Map visualization: No data or pos_df is empty.") # Fixed: Added else block

        print(f"\n📁 All visualizations saved to: analysis_visualizations/")

    def save_analysis_results(self):
        """Save all analysis results to files"""
        print("\n💾 STEP 7: SAVING ANALYSIS RESULTS")
        print("-" * 50)

        import json
        import pickle
        import os # Import os module here

        # 1. Save insights as JSON
        insights_file = 'analysis_results/insights_summary.json'
        os.makedirs('analysis_results', exist_ok=True)

        with open(insights_file, 'w') as f:
            # Convert DataFrames to dict for JSON serialization
            json_ready_insights = {}
            for key, value in self.insights.items():
                if isinstance(value, (pd.DataFrame, pd.Series)):
                    json_ready_insights[key] = value.to_dict()
                else:
                    json_ready_insights[key] = value

            json.dump(json_ready_insights, f, indent=2, default=str)

        print(f"  ✅ Saved insights: {insights_file}")

        # 2. Save full analysis results as pickle
        results_file = 'analysis_results/full_analysis_results.pkl'
        with open(results_file, 'wb') as f:
            pickle.dump(self.analysis_results, f)

        print(f"  ✅ Saved full results: {results_file}")

        # 3. Create executive summary report
        self._create_executive_summary()

        # 4. Save analysis-ready data
        analysis_data_file = 'analysis_results/analysis_ready_data.csv'
        self.df.to_csv(analysis_data_file, index=False)
        print(f"  ✅ Saved analysis-ready data: {analysis_data_file}")

        print(f"\n📁 All analysis results saved to: analysis_results/")

    def _create_executive_summary(self):
        """Create an executive summary report"""
        summary_content = f"""# EXECUTIVE SUMMARY: Eco-Friendly Market Intelligence
## Analysis Date: {datetime.now().strftime('%Y-%m-%d')}
## Dataset: {len(self.df):,} Products Analyzed

## KEY FINDINGS

### 1. Market Overview
- **Total Products Analyzed**: {len(self.df):,}
- **Average Price**: ${self.df['price'].mean():.2f}
- **Average Rating**: {self.df['rating'].mean():.2f}/5
- **Market Discount Rate**: {(self.df['on_sale'].mean()*100):.1f}% of products on sale

### 2. Top Performing Categories
"""

        # Add category performance
        pricing_data = self.analysis_results.get('pricing_intelligence', {})
        if 'category_pricing' in pricing_data and not pricing_data['category_pricing'].empty:
            top_categories = pricing_data['category_pricing'].head(3)
            for idx, row in top_categories.iterrows():
                summary_content += f"- **{idx}**: ${row['price_mean']:.2f} average price, {row['price_count']} products\n"

        summary_content += """
### 3. Competitive Landscape
"""

        # Add competitor insights
        competitor_data = self.analysis_results.get('competitor_analysis', {})
        if 'brand_performance' in competitor_data and not competitor_data['brand_performance'].empty:
            top_brands = competitor_data['brand_performance'].head(3)
            for brand, row in top_brands.iterrows():
                summary_content += f"- **{brand}**: Success Score {row['success_score']:.3f}, ${row['price']:.2f} avg price\n"

        summary_content += """
### 4. Consumer Preferences
"""

        # Add attribute insights
        trends_data = self.analysis_results.get('market_trends', {})
        if 'attribute_premiums' in trends_data and not trends_data['attribute_premiums'].empty:
            top_attrs = trends_data['attribute_premiums'].head(3)
            for _, row in top_attrs.iterrows():
                summary_content += f"- **{row['attribute']}**: Commands {row['premium_pct']:.1f}% price premium\n"

        summary_content += """
### 5. Growth Opportunities
"""

        # Add opportunity insights
        if 'category_opportunities' in trends_data and not trends_data['category_opportunities'].empty:
            top_opp = trends_data['category_opportunities'].iloc[0]
            summary_content += f"- **{trends_data['category_opportunities'].index[0]}**: Highest growth opportunity (Score: {top_opp['opportunity_score']:.3f})\n"

        summary_content += f"""
## STRATEGIC RECOMMENDATIONS

"""

        # Add recommendations
        if 'strategic_recommendations' in self.insights: 
            for i, rec in enumerate(self.insights['strategic_recommendations'][:5], 1):
                summary_content += f"{i}. {rec}\n"

        summary_content += f"""
## ANALYSIS SCOPE
- **Time Period**: {self.df['date_collected'].min().strftime('%Y-%m-%d') if 'date_collected' in self.df.columns and not self.df['date_collected'].isnull().all() else 'Current'} 
- **Categories Covered**: {self.df['category'].nunique()}
- **Brands Analyzed**: {self.df['brand'].nunique()}
- **Data Sources**: {self.df['website'].nunique()} websites

## NEXT STEPS
1. Implement pricing strategy based on category analysis
2. Develop products with high-value sustainability attributes
3. Target under-served market categories
4. Monitor competitor movements in premium segments

---
*Report generated automatically by Market Intelligence System*
"""

        with open('analysis_results/executive_summary.md', 'w') as f:
            f.write(summary_content)

        print("  ✅ Created: Executive summary report")

# Main execution
if __name__ == "__main__":
    # Initialize analyzer
    analyzer = MarketIntelligenceAnalyzer(data_file='clean_master_dataset.csv')

    # Run complete analysis pipeline
    results, insights = analyzer.run_analysis_pipeline()

    # Display completion message
    print("\n" + "="*80)
    print("📊 ANALYSIS COMPLETE - READY FOR REPORTING")
    print("="*80)
    print(f"\n📁 Results saved in: analysis_results/")
    print(f"📈 Visualizations in: analysis_visualizations/")
    print(f"\n➡️ Next: Proceed to Phase 4 - Reporting & Dashboard Creation")
