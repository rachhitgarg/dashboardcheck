# 🚀 AI Initiatives Impact Dashboard - SP Jain School of Global Management

## Overview
This comprehensive dashboard provides insights into the impact of various AI initiatives implemented at SP Jain School of Global Management across MGB and GMBA programs. The dashboard enables stakeholders to analyze performance metrics, compare traditional vs. AI-enhanced approaches, and make data-driven decisions.

## 🎯 Objectives Addressed

### 1. **Job Conversion Analysis**
- Track placement success rates across different AI tool usage levels
- Monitor conversion rates from application to selection
- Analyze year-over-year placement trends

### 2. **Better Companies and Better Packages**
- Company tier analysis (Tier 1, 2, 3)
- Industry-wise CTC distribution
- Quality vs. quantity of opportunities

### 3. **Traditional vs. AI Implementation Comparison**
- Student engagement metrics
- Learning outcomes comparison
- Faculty efficiency analysis
- Placement success rates
- Student satisfaction scores

### 4. **AI Tutor Usage and Impact**
- Student adoption rates
- Faculty feedback analysis
- Session creation and participation metrics
- Performance correlation with usage levels

### 5. **Higher Degree Examinations & Assignments**
- Unit-wise performance analysis
- Component-wise scoring (CP, IA, GA, TE)
- Year-over-year performance trends

### 6. **AI Mentor Impact and Usage**
- Academic manager feedback
- Student motivation levels
- Project performance improvements
- Effectiveness ratings

### 7. **JPT Impact on Placement**
- Pre and post-JPT implementation analysis
- Interview to selection conversion rates
- Company visit vs. opportunity correlation

## 📊 Data Sources & Structure

### **AI Tutor Usage Summary Template**
- **Data Type**: Session-level analytics
- **Key Metrics**: 
  - Student adoption rate
  - Session creation and participation
  - Faculty ratings and feedback
  - Quiz outcomes and usage
- **Records**: 270 mock records (3 years × 2 programs × 3 campuses × 5 units × 3 cohorts)

### **AI Mentor Feedback Template**
- **Data Type**: Academic manager feedback
- **Key Metrics**:
  - Improvement observation rates
  - Student motivation levels
  - Effectiveness ratings
  - Key observations and insights
- **Records**: 100 mock records (3 years × 2 programs × 3-8 managers per cohort)

### **AI Initiatives Impact Data**
- **Data Type**: Student-level performance data
- **Key Metrics**:
  - Placement status
  - CGPA scores
  - AI tool usage levels
  - Program and cohort information
- **Records**: 1,800 mock records (3 years × 2 programs × 3 cohorts × 100 students)

### **JPT Usage Analysis & Placement Tracker**
- **Data Type**: Company and placement data
- **Key Metrics**:
  - Company information and tiers
  - Vacancies offered vs. selections
  - CTC ranges and industry distribution
  - Conversion rates
- **Records**: 270 mock records (3 years × 2 programs × 3 cohorts × 5-10 companies)

### **Unit-wise Performance Analysis**
- **Data Type**: Academic performance data
- **Key Metrics**:
  - Component scores (CP, IA, GA, TE)
  - Total scores and trends
  - Year-over-year comparisons
- **Records**: 90 mock records (3 years × 2 programs × 3 cohorts × 5 units)

## 🛠️ Technical Implementation

### **Technology Stack**
- **Frontend**: Streamlit (Python-based web framework)
- **Data Processing**: Pandas, NumPy
- **Visualization**: Plotly (interactive charts)
- **Data Storage**: CSV files (easily convertible to database)

### **Key Features**
1. **Interactive Filters**
   - Year selection (2022-2024)
   - Program selection (MGB/GMBA)
   - Campus selection (SG/DXB/MUM)
   - AI tool selection

2. **Real-time Analytics**
   - Dynamic metric calculations
   - Filtered data views
   - Responsive visualizations

3. **Comprehensive Visualizations**
   - Line charts for trends
   - Bar charts for comparisons
   - Pie charts for distributions
   - Subplots for multi-metric views

## 📈 Key Metrics & KPIs

### **AI Tutor Metrics**
- Student adoption rate: **70-95%**
- Average rating: **3.5-4.9/5.0**
- Session participation: **80-120 students per unit**
- Faculty satisfaction: **85% positive feedback**

### **AI Mentor Metrics**
- Improvement observation rate: **75-90%**
- Student motivation rate: **80-85%**
- Effectiveness rating: **70-85% very effective**

### **JPT Metrics**
- Conversion rate: **15-35%**
- Average CTC: **₹8-25 LPA**
- Company tier distribution: **Tier 1: 30%, Tier 2: 45%, Tier 3: 25%**

### **Overall Impact**
- Placement rate improvement: **14-22%**
- Student engagement increase: **20%**
- Learning outcomes enhancement: **18%**

## 🔍 CR Team Challenge Counter

### **Challenge Scenario**
"Last time 10 companies visited and 5 selections happened. This time 20 companies visited still 5 selections."

### **Data-Driven Response**
The dashboard provides comprehensive metrics to counter such claims:

1. **Conversion Rate Analysis**
   - Focus on quality, not quantity
   - Track interview-to-selection ratios
   - Monitor vacancy-to-selection efficiency

2. **Market Context**
   - Economic conditions affecting vacancy numbers
   - Industry-specific hiring trends
   - Company-specific opportunity counts

3. **Performance Metrics**
   - Year-over-year conversion rate trends
   - Company tier quality analysis
   - Student preparation effectiveness

## 🚀 Deployment & Usage

### **Local Development**
```bash
# Install dependencies
pip install -r requirements.txt

# Run dashboard
streamlit run ai_initiatives_dashboard.py
```

### **Streamlit Cloud Deployment**
1. Push code to GitHub repository
2. Connect repository to Streamlit Cloud
3. Deploy with automatic updates

### **Data Updates**
- Replace CSV files with new data
- Maintain consistent column structure
- Update mock data generation script as needed

## 📋 Dashboard Sections

### **1. AI Tutor Impact Analysis**
- Adoption rates and trends
- Faculty feedback distribution
- Session metrics and participation

### **2. AI Mentor Impact Analysis**
- Effectiveness ratings
- Improvement observations
- Student motivation levels

### **3. JPT Impact Analysis**
- Placement performance
- Company analysis
- Year-over-year comparisons
- CR team challenge counter

### **4. Overall AI Impact**
- Tool usage correlation with placement
- CGPA vs. AI usage analysis
- Traditional vs. AI comparison

### **5. Unit Performance Analysis**
- Academic performance trends
- Component-wise scoring
- Cohort comparisons

## 🔧 Customization & Extension

### **Adding New Metrics**
1. Update data generation script
2. Add new visualization functions
3. Integrate with existing dashboard sections

### **Data Source Integration**
- Database connections (PostgreSQL, MySQL)
- API integrations
- Real-time data streaming

### **Additional Features**
- Export functionality
- Email reporting
- Mobile optimization
- Advanced analytics (ML models)

## 📊 Data Quality & Validation

### **Mock Data Characteristics**
- **Realistic Ranges**: Based on typical business school metrics
- **Consistent Relationships**: Logical correlations between metrics
- **Temporal Patterns**: Year-over-year trends and seasonality
- **Program Variations**: MGB vs. GMBA differences

### **Data Validation Rules**
- Adoption rates: 0-100%
- Ratings: 1.0-5.0 scale
- CTC ranges: ₹5-50 LPA
- Conversion rates: 0-100%

## 🎯 Future Enhancements

### **Phase 2 Features**
- Predictive analytics for placement success
- Student performance forecasting
- ROI analysis for AI initiatives
- Comparative benchmarking with peer institutions

### **Phase 3 Features**
- Machine learning insights
- Natural language processing for feedback
- Real-time dashboard updates
- Mobile application

## 📞 Support & Maintenance

### **Technical Support**
- Python/Streamlit expertise required
- Data science background recommended
- Regular dependency updates needed

### **Data Maintenance**
- Monthly data refresh cycles
- Quarterly metric validation
- Annual performance reviews

---

**Developed for**: SP Jain School of Global Management  
**Purpose**: AI Initiatives Impact Analysis & Decision Support  
**Last Updated**: September 2024  
**Version**: 1.0.0
