import pandas as pd
import numpy as np
import os

def test_data_files():
    """Test if all required data files exist and are readable"""
    required_files = [
        'ai_tutor_mock_data.csv',
        'ai_mentor_mock_data.csv',
        'ai_impact_mock_data.csv',
        'jpt_mock_data.csv',
        'unit_performance_mock_data.csv'
    ]
    
    print("🔍 Testing data files...")
    
    for file in required_files:
        if os.path.exists(file):
            try:
                df = pd.read_csv(file)
                print(f"✅ {file}: {len(df)} records, {len(df.columns)} columns")
                
                # Check for missing values
                missing_count = df.isnull().sum().sum()
                if missing_count > 0:
                    print(f"   ⚠️  {missing_count} missing values found")
                else:
                    print(f"   ✅ No missing values")
                    
            except Exception as e:
                print(f"❌ {file}: Error reading file - {e}")
        else:
            print(f"❌ {file}: File not found")
    
    print("\n" + "="*50)

def test_data_quality():
    """Test data quality and consistency"""
    print("🔍 Testing data quality...")
    
    try:
        # Test AI Tutor data
        ai_tutor = pd.read_csv('ai_tutor_mock_data.csv')
        print("✅ AI Tutor data loaded successfully")
        
        # Check adoption rate range
        adoption_range = (ai_tutor['Student_adoption_rate'].min(), ai_tutor['Student_adoption_rate'].max())
        print(f"   📊 Adoption rate range: {adoption_range[0]:.1f}% - {adoption_range[1]:.1f}%")
        
        # Check rating range
        rating_range = (ai_tutor['Avg_Rating_for_AI_Tutor_Tool'].min(), ai_tutor['Avg_Rating_for_AI_Tutor_Tool'].max())
        print(f"   ⭐ Rating range: {rating_range[0]:.1f} - {rating_range[1]:.1f}")
        
    except Exception as e:
        print(f"❌ Error testing AI Tutor data: {e}")
    
    try:
        # Test JPT data
        jpt_data = pd.read_csv('jpt_mock_data.csv')
        print("✅ JPT data loaded successfully")
        
        # Check conversion rates
        jpt_data['conversion_rate'] = (jpt_data['Students_Selected'] / jpt_data['Students_Interviewed'] * 100).fillna(0)
        conversion_range = (jpt_data['conversion_rate'].min(), jpt_data['conversion_rate'].max())
        print(f"   📈 Conversion rate range: {conversion_range[0]:.1f}% - {conversion_range[1]:.1f}%")
        
        # Check CTC ranges
        ctc_range = (jpt_data['Avg_CTC'].min(), jpt_data['Avg_CTC'].max())
        print(f"   💰 CTC range: ₹{ctc_range[0]:,.0f} - ₹{ctc_range[1]:,.0f}")
        
    except Exception as e:
        print(f"❌ Error testing JPT data: {e}")
    
    try:
        # Test AI Impact data
        ai_impact = pd.read_csv('ai_impact_mock_data.csv')
        print("✅ AI Impact data loaded successfully")
        
        # Check placement rates
        placement_rate = (ai_impact['Placed_Not_Placed'] == 'Placed').mean() * 100
        print(f"   🎯 Overall placement rate: {placement_rate:.1f}%")
        
        # Check CGPA range
        cgpa_range = (ai_impact['CGPA'].min(), ai_impact['CGPA'].max())
        print(f"   📚 CGPA range: {cgpa_range[0]:.2f} - {cgpa_range[1]:.2f}")
        
    except Exception as e:
        print(f"❌ Error testing AI Impact data: {e}")
    
    print("\n" + "="*50)

def test_dashboard_requirements():
    """Test if dashboard requirements can be met"""
    print("🔍 Testing dashboard requirements...")
    
    try:
        # Test if we can create basic visualizations
        import plotly.express as px
        import plotly.graph_objects as go
        print("✅ Plotly libraries available")
        
        # Test if we can create a sample chart
        sample_data = pd.DataFrame({'x': [1, 2, 3], 'y': [4, 5, 6]})
        fig = px.line(sample_data, x='x', y='y')
        print("✅ Sample chart creation successful")
        
    except Exception as e:
        print(f"❌ Error testing visualization capabilities: {e}")
    
    try:
        # Test Streamlit availability
        import streamlit as st
        print("✅ Streamlit available")
    except Exception as e:
        print(f"❌ Streamlit not available: {e}")
    
    print("\n" + "="*50)

def main():
    """Run all tests"""
    print("🚀 AI Initiatives Dashboard - Data & System Test")
    print("="*50)
    
    test_data_files()
    test_data_quality()
    test_dashboard_requirements()
    
    print("🎉 Testing completed!")
    print("\n📋 Next steps:")
    print("1. Run: streamlit run ai_initiatives_dashboard.py")
    print("2. Open browser to the displayed URL")
    print("3. Test all dashboard features and filters")

if __name__ == "__main__":
    main()
