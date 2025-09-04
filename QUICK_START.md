# 🚀 Quick Start Guide - AI Initiatives Dashboard

## ⚡ Get Started in 5 Minutes

### **1. Install Dependencies**
```bash
pip install -r requirements.txt
```

### **2. Run Dashboard**
```bash
streamlit run ai_initiatives_dashboard.py
```

### **3. Open Browser**
Navigate to: `http://localhost:8501`

## 📊 Dashboard Features

### **🎯 Key Metrics**
- **AI Tutor**: Student adoption rates, faculty feedback, session metrics
- **AI Mentor**: Effectiveness ratings, improvement observations
- **JPT**: Placement performance, conversion rates, company analysis
- **Overall Impact**: Traditional vs. AI comparison, performance trends

### **🔍 Interactive Filters**
- **Years**: 2022-2024
- **Programs**: MGB, GMBA
- **Campuses**: SG, DXB, MUM
- **AI Tools**: Individual or all tools

### **📈 Visualizations**
- Line charts for trends
- Bar charts for comparisons
- Pie charts for distributions
- Multi-metric subplots

## 🎯 CR Team Challenge Counter

### **Challenge**: "More companies, same selections"
### **Response**: Focus on conversion rates, not just company count

The dashboard automatically calculates and displays:
- Interview-to-selection conversion rates
- Year-over-year performance trends
- Company tier quality analysis
- Market context insights

## 📁 Project Structure

```
Templates for Dashboard/
├── ai_initiatives_dashboard.py    # Main dashboard application
├── generate_mock_data.py          # Mock data generator
├── test_dashboard.py              # Data and system tests
├── requirements.txt               # Python dependencies
├── README.md                      # Comprehensive documentation
├── DEPLOYMENT.md                  # Deployment guide
├── QUICK_START.md                 # This file
└── Mock Data Files:
    ├── ai_tutor_mock_data.csv
    ├── ai_mentor_mock_data.csv
    ├── ai_impact_mock_data.csv
    ├── jpt_mock_data.csv
    └── unit_performance_mock_data.csv
```

## 🔧 Customization

### **Add Real Data**
1. Replace CSV files with your data
2. Maintain column structure
3. Update data loading if needed

### **Modify Metrics**
1. Edit visualization functions
2. Add new KPIs
3. Customize filters

## 🚨 Troubleshooting

### **Dashboard Won't Start**
```bash
# Check dependencies
pip install -r requirements.txt

# Check port availability
netstat -an | findstr :8501
```

### **Data Loading Errors**
```bash
# Test data files
python test_dashboard.py

# Regenerate mock data
python generate_mock_data.py
```

### **Performance Issues**
- Reduce data size
- Use data sampling
- Implement caching

## 📱 Mobile Access

- Dashboard is fully responsive
- Works on all device sizes
- Touch-friendly interface

## 🚀 Deployment

### **Local Development**
```bash
streamlit run ai_initiatives_dashboard.py
```

### **Streamlit Cloud**
1. Push to GitHub
2. Connect to Streamlit Cloud
3. Deploy automatically

## 📞 Support

- Check `README.md` for detailed documentation
- Review `DEPLOYMENT.md` for deployment help
- Run `test_dashboard.py` for diagnostics

---

**Ready to go!** 🎉  
The dashboard will show comprehensive insights into your AI initiatives' impact.
