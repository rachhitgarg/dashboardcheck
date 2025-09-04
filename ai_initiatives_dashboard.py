import streamlit as st
import pandas as pd
import numpy as np
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import warnings
warnings.filterwarnings('ignore')
from data_manager import DataManager
import os

# Page configuration
st.set_page_config(
    page_title="AI Initiatives Dashboard - SP Jain",
    page_icon="🚀",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom CSS
st.markdown("""
<style>
    .main-header {
        font-size: 2.5rem;
        color: #1f77b4;
        text-align: center;
        margin-bottom: 2rem;
        font-weight: bold;
    }
    .metric-card {
        background-color: #f0f2f6;
        padding: 1rem;
        border-radius: 0.5rem;
        border-left: 4px solid #1f77b4;
    }
    .tool-toggle {
        background-color: #e1f5fe;
        padding: 1rem;
        border-radius: 0.5rem;
        margin: 1rem 0;
    }
</style>
""", unsafe_allow_html=True)

@st.cache_data
def load_data():
    """Load all the mock data files"""
    try:
        ai_tutor = pd.read_csv('ai_tutor_mock_data.csv')
        ai_mentor = pd.read_csv('ai_mentor_mock_data.csv')
        ai_impact = pd.read_csv('ai_impact_mock_data.csv')
        jpt_data = pd.read_csv('jpt_mock_data.csv')
        unit_performance = pd.read_csv('unit_performance_mock_data.csv')
        
        return ai_tutor, ai_mentor, ai_impact, jpt_data, unit_performance
    except Exception as e:
        st.error(f"Error loading data: {e}")
        return None, None, None, None, None

def calculate_conversion_rate(selected, applied):
    """Calculate conversion rate with error handling"""
    if applied == 0:
        return 0
    return (selected / applied) * 100

def data_management_page():
    """Data Management Page for uploading, downloading, and managing data"""
    st.markdown('<h1 class="main-header">📊 Data Management Center</h1>', unsafe_allow_html=True)
    st.markdown("### Upload, Download, and Manage AI Initiatives Data")
    
    # Initialize data manager
    data_manager = DataManager()
    
    # Create tabs for different operations
    tab1, tab2, tab3, tab4 = st.tabs(["📥 Download Templates", "📤 Upload Data", "🗂️ Data Summary", "📋 Operation Logs"])
    
    with tab1:
        st.subheader("📥 Download Data Templates")
        st.write("Download empty templates to fill with your data:")
        
        col1, col2 = st.columns(2)
        
        with col1:
            # Individual template downloads
            st.write("**Individual Templates:**")
            for data_type in data_manager.templates.keys():
                template_data = data_manager.download_template(data_type)
                if template_data:
                    st.download_button(
                        label=f"📄 Download {data_type} Template",
                        data=template_data,
                        file_name=data_manager.templates[data_type]['filename'],
                        mime='text/csv',
                        key=f"download_{data_type.replace(' ', '_')}"
                    )
        
        with col2:
            # All templates download
            st.write("**All Templates (ZIP):**")
            all_templates = data_manager.download_all_templates()
            st.download_button(
                label="📦 Download All Templates (ZIP)",
                data=all_templates,
                file_name="ai_initiatives_templates.zip",
                mime='application/zip'
            )
            
            st.info("""
            **Instructions:**
            1. Download the template(s) you need
            2. Fill in your data following the column structure
            3. Save as CSV format
            4. Upload using the 'Upload Data' tab
            """)
    
    with tab2:
        st.subheader("📤 Upload Data")
        
        # User information
        col1, col2 = st.columns(2)
        with col1:
            user_name = st.text_input("Your Name", placeholder="Enter your name for logging")
        with col2:
            user_team = st.text_input("Team/Department", placeholder="e.g., Academic Team, Placement Team")
        
        user_info = f"{user_name} ({user_team})" if user_name and user_team else "Anonymous User"
        
        # Data type selection
        data_type = st.selectbox("Select Data Type", list(data_manager.templates.keys()))
        
        # File upload
        uploaded_file = st.file_uploader(
            f"Upload {data_type} Data",
            type=['csv'],
            help=f"Upload CSV file with {data_type} data"
        )
        
        if uploaded_file is not None:
            try:
                # Read uploaded file
                uploaded_df = pd.read_csv(uploaded_file)
                
                st.write("**Preview of uploaded data:**")
                st.dataframe(uploaded_df.head())
                
                # Validate data structure
                is_valid, message = data_manager.validate_uploaded_data(uploaded_df, data_type)
                
                if is_valid:
                    st.success(f"✅ {message}")
                    
                    # Load existing data
                    existing_df = data_manager.load_existing_data(data_type)
                    
                    st.write(f"**Current data:** {len(existing_df)} records")
                    st.write(f"**New data:** {len(uploaded_df)} records")
                    
                    # Operation selection
                    operation = st.radio(
                        "Choose operation:",
                        ["Merge with existing data", "Replace all existing data"],
                        help="Merge: Add new data to existing data. Replace: Delete all existing data and use only new data."
                    )
                    
                    col1, col2 = st.columns(2)
                    
                    with col1:
                        if st.button("🚀 Execute Upload", type="primary"):
                            if operation == "Merge with existing data":
                                result_df, success, msg = data_manager.merge_data(existing_df, uploaded_df, data_type, user_info)
                            else:
                                result_df, success, msg = data_manager.replace_data(uploaded_df, data_type, user_info)
                            
                            if success:
                                # Save the data
                                save_success, save_msg = data_manager.save_data(result_df, data_type)
                                if save_success:
                                    st.success(f"✅ {msg}")
                                    st.success(f"✅ {save_msg}")
                                    st.balloons()
                                    
                                    # Clear cache to reload data
                                    st.cache_data.clear()
                                else:
                                    st.error(f"❌ {save_msg}")
                            else:
                                st.error(f"❌ {msg}")
                    
                    with col2:
                        if st.button("🗑️ Delete All Data", help="This will delete all existing data for this type"):
                            if st.checkbox("I confirm I want to delete all data", key="delete_confirm"):
                                success, msg = data_manager.delete_data(data_type, user_info)
                                if success:
                                    st.success(f"✅ {msg}")
                                    st.cache_data.clear()
                                else:
                                    st.error(f"❌ {msg}")
                
                else:
                    st.error(f"❌ {message}")
                    st.write("**Expected columns:**")
                    st.write(data_manager.templates[data_type]['columns'])
                    
            except Exception as e:
                st.error(f"❌ Error reading uploaded file: {e}")
    
    with tab3:
        st.subheader("🗂️ Data Summary")
        
        summary = data_manager.get_data_summary()
        
        # Display as cards
        cols = st.columns(2)
        for i, (data_type, info) in enumerate(summary.items()):
            with cols[i % 2]:
                if 'error' in info:
                    st.error(f"**{data_type}**\n\nError: {info['error']}")
                elif 'status' in info:
                    st.warning(f"**{data_type}**\n\n{info['status']}")
                else:
                    st.info(f"""
                    **{data_type}**
                    
                    📊 Records: {info['records']:,}
                    📅 Last Modified: {info['last_modified']}
                    💾 File Size: {info['file_size']}
                    """)
        
        # Refresh button
        if st.button("🔄 Refresh Summary"):
            st.cache_data.clear()
            st.experimental_rerun()
    
    with tab4:
        st.subheader("📋 Operation Logs")
        
        if 'operation_logs' in st.session_state and st.session_state.operation_logs:
            # Display recent logs
            st.write("**Recent Operations:**")
            logs_df = pd.DataFrame(st.session_state.operation_logs)
            
            # Sort by timestamp (most recent first)
            logs_df = logs_df.sort_values('timestamp', ascending=False)
            
            # Display as table
            st.dataframe(
                logs_df,
                use_container_width=True,
                hide_index=True
            )
            
            # Clear logs button
            if st.button("🗑️ Clear Logs"):
                st.session_state.operation_logs = []
                st.experimental_rerun()
                
        else:
            st.info("No operations logged yet.")
        
        # Download full log file
        if os.path.exists('data_operations.log'):
            with open('data_operations.log', 'r') as f:
                log_content = f.read()
            
            st.download_button(
                label="📄 Download Full Log File",
                data=log_content,
                file_name=f"data_operations_log_{pd.Timestamp.now().strftime('%Y%m%d')}.txt",
                mime='text/plain'
            )

def main():
    # Sidebar navigation
    st.sidebar.title("🚀 Navigation")
    page = st.sidebar.selectbox(
        "Choose a page:",
        ["📈 Dashboard", "📊 Data Management"]
    )
    
    if page == "📊 Data Management":
        data_management_page()
        return
    
    # Header
    st.markdown('<h1 class="main-header">🚀 AI Initiatives Impact Dashboard</h1>', unsafe_allow_html=True)
    st.markdown("### SP Jain School of Global Management - MGB & GMBA Programs")
    
    # Load data
    ai_tutor, ai_mentor, ai_impact, jpt_data, unit_performance = load_data()
    
    if ai_tutor is None:
        st.error("Failed to load data. Please check if all CSV files are present.")
        return
    
    # Sidebar for filters
    st.sidebar.header("📊 Dashboard Filters")
    
    # Year filter with dropdown
    years = sorted(ai_impact['Year'].unique()) if 'Year' in ai_impact.columns else [2022, 2023, 2024]
    
    # Create a container for better styling
    with st.sidebar.container():
        st.write("**📅 Year Selection:**")
        year_options = ["All Years"] + [str(year) for year in years]
        selected_year_option = st.selectbox("Choose Years", year_options, index=0)
        
        if selected_year_option == "All Years":
            selected_years = years
        else:
            selected_years = [int(selected_year_option)]
        
        # Additional multi-select for custom selection
        if st.checkbox("Custom Year Selection"):
            selected_years = st.multiselect("Select specific years", years, default=years[-3:])
    
    # Program filter with dropdown
    programs = sorted(ai_impact['Program'].unique()) if 'Program' in ai_impact.columns else ['MGB', 'GMBA']
    
    with st.sidebar.container():
        st.write("**🎓 Program Selection:**")
        program_options = ["All Programs"] + programs
        selected_program_option = st.selectbox("Choose Programs", program_options, index=0)
        
        if selected_program_option == "All Programs":
            selected_programs = programs
        else:
            selected_programs = [selected_program_option]
        
        # Additional multi-select for custom selection
        if st.checkbox("Custom Program Selection"):
            selected_programs = st.multiselect("Select specific programs", programs, default=programs)
    
    # Campus filter with dropdown
    campuses = sorted(ai_tutor['Campus'].unique()) if 'Campus' in ai_tutor.columns else ['SG', 'DXB', 'MUM']
    
    with st.sidebar.container():
        st.write("**🏫 Campus Selection:**")
        campus_options = ["All Campuses"] + campuses
        selected_campus_option = st.selectbox("Choose Campuses", campus_options, index=0)
        
        if selected_campus_option == "All Campuses":
            selected_campuses = campuses
        else:
            selected_campuses = [selected_campus_option]
        
        # Additional multi-select for custom selection
        if st.checkbox("Custom Campus Selection"):
            selected_campuses = st.multiselect("Select specific campuses", campuses, default=campuses)
    
    # Tool selection with improved interface
    st.sidebar.header("🛠️ AI Tools Analysis")
    with st.sidebar.container():
        tool_options = ["All Tools", "AI Tutor", "AI Mentor", "JPT"]
        selected_tool_option = st.selectbox("Choose AI Tools", tool_options, index=0)
        
        if selected_tool_option == "All Tools":
            selected_tools = ["AI Tutor", "AI Mentor", "JPT", "All Tools"]
        else:
            selected_tools = [selected_tool_option]
        
        # Additional multi-select for custom selection
        if st.checkbox("Custom Tool Selection"):
            selected_tools = st.multiselect(
                "Select specific tools",
                ["AI Tutor", "AI Mentor", "JPT", "All Tools"],
                default=["All Tools"]
            )
    
    # Filter summary
    st.sidebar.markdown("---")
    st.sidebar.write("**🔍 Current Filters:**")
    st.sidebar.write(f"📅 Years: {len(selected_years)} selected")
    st.sidebar.write(f"🎓 Programs: {len(selected_programs)} selected") 
    st.sidebar.write(f"🏫 Campuses: {len(selected_campuses)} selected")
    st.sidebar.write(f"🛠️ Tools: {len([t for t in selected_tools if t != 'All Tools'])} selected")
    
    # Reset filters button
    if st.sidebar.button("🔄 Reset All Filters"):
        st.experimental_rerun()
    
    # Apply filters
    if selected_years:
        ai_tutor_filtered = ai_tutor[ai_tutor['Year'].isin(selected_years)] if 'Year' in ai_tutor.columns else ai_tutor
        ai_mentor_filtered = ai_mentor[ai_mentor['Year'].isin(selected_years)] if 'Year' in ai_mentor.columns else ai_mentor
        ai_impact_filtered = ai_impact[ai_impact['Year'].isin(selected_years)] if 'Year' in ai_impact.columns else ai_impact
        jpt_filtered = jpt_data[jpt_data['Year'].isin(selected_years)] if 'Year' in jpt_data.columns else jpt_data
        unit_performance_filtered = unit_performance[unit_performance['Year'].isin(selected_years)] if 'Year' in unit_performance.columns else unit_performance
    else:
        ai_tutor_filtered, ai_mentor_filtered, ai_impact_filtered, jpt_filtered, unit_performance_filtered = ai_tutor, ai_mentor, ai_impact, jpt_data, unit_performance
    
    if selected_programs:
        ai_tutor_filtered = ai_tutor_filtered[ai_tutor_filtered['Course_Name'].isin(selected_programs)]
        ai_mentor_filtered = ai_mentor_filtered[ai_mentor_filtered['Program'].isin(selected_programs)]
        ai_impact_filtered = ai_impact_filtered[ai_impact_filtered['Program'].isin(selected_programs)]
        jpt_filtered = jpt_filtered[jpt_filtered['Program'].isin(selected_programs)]
        unit_performance_filtered = unit_performance_filtered[unit_performance_filtered['Program'].isin(selected_programs)]
    
    if selected_campuses:
        ai_tutor_filtered = ai_tutor_filtered[ai_tutor_filtered['Campus'].isin(selected_campuses)]
    
    # Main dashboard content
    if "All Tools" in selected_tools or "AI Tutor" in selected_tools:
        st.header("📚 AI Tutor Impact Analysis")
        
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            avg_adoption = ai_tutor_filtered['Student_adoption_rate'].mean()
            st.metric("Avg Student Adoption Rate", f"{avg_adoption:.1f}%")
        
        with col2:
            avg_rating = ai_tutor_filtered['Avg_Rating_for_AI_Tutor_Tool'].mean()
            st.metric("Avg AI Tutor Rating", f"{avg_rating:.2f}/5.0")
        
        with col3:
            total_sessions = ai_tutor_filtered['No_of_Session_IDs_created'].sum()
            st.metric("Total Sessions Created", f"{total_sessions:,}")
        
        with col4:
            total_participants = ai_tutor_filtered['Total_Students_Participated'].sum()
            st.metric("Total Students Participated", f"{total_participants:,}")
        
        # AI Tutor Usage Trends
        col1, col2 = st.columns(2)
        
        with col1:
            # Adoption rate by year
            if 'Year' in ai_tutor_filtered.columns:
                adoption_by_year = ai_tutor_filtered.groupby('Year')['Student_adoption_rate'].mean().reset_index()
                fig = px.line(adoption_by_year, x='Year', y='Student_adoption_rate', 
                            title='Student Adoption Rate Trend',
                            markers=True)
                fig.update_layout(height=400)
                st.plotly_chart(fig, use_container_width=True)
        
        with col2:
            # Rating distribution with grouped categories
            ratings = ai_tutor_filtered['Avg_Rating_for_AI_Tutor_Tool']
            
            # Create meaningful rating groups
            def group_ratings(rating):
                if rating >= 4.5:
                    return "⭐ Excellent (4.5-5.0)"
                elif rating >= 4.0:
                    return "✅ Good (4.0-4.4)"
                elif rating >= 3.5:
                    return "📊 Average (3.5-3.9)"
                elif rating >= 3.0:
                    return "⚠️ Fair (3.0-3.4)"
                else:
                    return "❌ Poor (<3.0)"
            
            # Group the ratings
            grouped_ratings = ratings.apply(group_ratings)
            rating_group_counts = grouped_ratings.value_counts()
            
            # Create horizontal bar chart for better readability
            fig = px.bar(
                x=rating_group_counts.values, 
                y=rating_group_counts.index,
                orientation='h',
                title='AI Tutor Rating Distribution (Grouped)',
                labels={'x': 'Number of Responses', 'y': 'Rating Category'},
                color=rating_group_counts.values,
                color_continuous_scale='RdYlGn'
            )
            
            # Customize appearance
            fig.update_layout(
                height=400,
                yaxis={'categoryorder': 'total ascending'},
                showlegend=False
            )
            
            # Add value labels on bars
            fig.update_traces(texttemplate='%{x}', textposition='outside')
            
            st.plotly_chart(fig, use_container_width=True)
        
        # Faculty feedback analysis
        st.subheader("Faculty Feedback Analysis")
        faculty_feedback = ai_tutor_filtered['Faculty_Feedback'].value_counts()
        fig = px.pie(values=faculty_feedback.values, names=faculty_feedback.index,
                    title='Faculty Feedback Distribution')
        st.plotly_chart(fig, use_container_width=True)
    
    if "All Tools" in selected_tools or "AI Mentor" in selected_tools:
        st.header("🤖 AI Mentor Impact Analysis")
        
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            total_managers = len(ai_mentor_filtered)
            st.metric("Total Academic Managers", total_managers)
        
        with col2:
            improvement_rate = (ai_mentor_filtered['Q1_Improvement_observed'].isin(['Strongly Agree', 'Agree']).sum() / len(ai_mentor_filtered)) * 100
            st.metric("Improvement Observed Rate", f"{improvement_rate:.1f}%")
        
        with col3:
            motivation_rate = (ai_mentor_filtered['Q2_Students_motivated'].isin(['Strongly Agree', 'Agree']).sum() / len(ai_mentor_filtered)) * 100
            st.metric("Student Motivation Rate", f"{motivation_rate:.1f}%")
        
        with col4:
            effectiveness_rate = (ai_mentor_filtered['Q3_Effectiveness'].isin(['Very Effective', 'Effective']).sum() / len(ai_mentor_filtered)) * 100
            st.metric("Effectiveness Rate", f"{effectiveness_rate:.1f}%")
        
        # AI Mentor effectiveness analysis
        col1, col2 = st.columns(2)
        
        with col1:
            effectiveness_dist = ai_mentor_filtered['Q3_Effectiveness'].value_counts()
            fig = px.bar(x=effectiveness_dist.index, y=effectiveness_dist.values,
                        title='AI Mentor Effectiveness Distribution',
                        labels={'x': 'Effectiveness Level', 'y': 'Count'})
            fig.update_layout(height=400)
            st.plotly_chart(fig, use_container_width=True)
        
        with col2:
            improvement_dist = ai_mentor_filtered['Q1_Improvement_observed'].value_counts()
            fig = px.pie(values=improvement_dist.values, names=improvement_dist.index,
                        title='Improvement Observation Distribution')
            fig.update_layout(height=400)
            st.plotly_chart(fig, use_container_width=True)
    
    if "All Tools" in selected_tools or "JPT" in selected_tools:
        st.header("💼 JPT (Job Preparation Tool) Impact Analysis")
        
        # Key metrics
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            total_companies = len(jpt_filtered['Company'].unique())
            st.metric("Total Companies", total_companies)
        
        with col2:
            total_vacancies = jpt_filtered['Vacancies_Offered'].sum()
            st.metric("Total Vacancies Offered", f"{total_vacancies:,}")
        
        with col3:
            total_selected = jpt_filtered['Students_Selected'].sum()
            st.metric("Total Students Selected", f"{total_selected:,}")
        
        with col4:
            avg_ctc = jpt_filtered['Avg_CTC'].mean()
            st.metric("Average CTC", f"₹{avg_ctc:,.0f}")
        
        # JPT Performance Analysis
        st.subheader("Placement Performance Analysis")
        
        col1, col2 = st.columns(2)
        
        with col1:
            # Conversion rate by company tier
            tier_conversion = jpt_filtered.groupby('Company_Tier').agg({
                'Students_Selected': 'sum',
                'Students_Interviewed': 'sum'
            }).reset_index()
            tier_conversion['Conversion_Rate'] = tier_conversion.apply(
                lambda x: calculate_conversion_rate(x['Students_Selected'], x['Students_Interviewed']), axis=1
            )
            
            fig = px.bar(tier_conversion, x='Company_Tier', y='Conversion_Rate',
                        title='Conversion Rate by Company Tier',
                        labels={'Conversion_Rate': 'Conversion Rate (%)'})
            fig.update_layout(height=400)
            st.plotly_chart(fig, use_container_width=True)
        
        with col2:
            # CTC distribution by industry
            industry_ctc = jpt_filtered.groupby('Industry_Sector')['Avg_CTC'].mean().reset_index()
            fig = px.bar(industry_ctc, x='Industry_Sector', y='Avg_CTC',
                        title='Average CTC by Industry',
                        labels={'Avg_CTC': 'Average CTC (₹)'})
            fig.update_layout(height=400, xaxis_tickangle=-45)
            st.plotly_chart(fig, use_container_width=True)
        
        # Year-over-year comparison
        st.subheader("Year-over-Year Performance Comparison")
        
        yearly_stats = jpt_filtered.groupby('Year').agg({
            'Company': 'nunique',
            'Vacancies_Offered': 'sum',
            'Students_Selected': 'sum',
            'Students_Interviewed': 'sum'
        }).reset_index()
        
        yearly_stats['Conversion_Rate'] = yearly_stats.apply(
            lambda x: calculate_conversion_rate(x['Students_Selected'], x['Students_Interviewed']), axis=1
        )
        
        # Create subplot for multiple metrics
        fig = make_subplots(
            rows=2, cols=2,
            subplot_titles=('Companies Visited', 'Vacancies Offered', 'Students Selected', 'Conversion Rate'),
            specs=[[{"secondary_y": False}, {"secondary_y": False}],
                   [{"secondary_y": False}, {"secondary_y": False}]]
        )
        
        fig.add_trace(go.Bar(x=yearly_stats['Year'], y=yearly_stats['Company'], name='Companies'), row=1, col=1)
        fig.add_trace(go.Bar(x=yearly_stats['Year'], y=yearly_stats['Vacancies_Offered'], name='Vacancies'), row=1, col=2)
        fig.add_trace(go.Bar(x=yearly_stats['Year'], y=yearly_stats['Students_Selected'], name='Selected'), row=2, col=1)
        fig.add_trace(go.Scatter(x=yearly_stats['Year'], y=yearly_stats['Conversion_Rate'], name='Conversion %', mode='lines+markers'), row=2, col=2)
        
        fig.update_layout(height=600, showlegend=False)
        st.plotly_chart(fig, use_container_width=True)
        
        # Intelligent Placement Quality Analysis (mindfully addressing concerns)
        st.subheader("🎯 Placement Quality & Market Efficiency Analysis")
        
        # Generate intelligent insights automatically
        col1, col2 = st.columns(2)
        
        with col1:
            # Market Context Analysis
            if len(yearly_stats) >= 2:
                current_year = yearly_stats.iloc[-1]
                previous_year = yearly_stats.iloc[-2]
                
                # Calculate key efficiency metrics
                companies_change = current_year['Company'] - previous_year['Company']
                conversion_change = current_year['Conversion_Rate'] - previous_year['Conversion_Rate']
                opportunities_per_company_current = current_year['Vacancies_Offered'] / current_year['Company']
                opportunities_per_company_previous = previous_year['Vacancies_Offered'] / previous_year['Company']
                
                st.info(f"""
                **📊 Market Efficiency Analysis:**
                
                • **Companies Engaged**: {current_year['Company']} (vs {previous_year['Company']} last year)
                • **Opportunities per Company**: {opportunities_per_company_current:.1f} (vs {opportunities_per_company_previous:.1f})
                • **Conversion Efficiency**: {current_year['Conversion_Rate']:.1f}% success rate
                • **Market Context**: {"Tighter job market" if opportunities_per_company_current < opportunities_per_company_previous else "Expanding opportunities"}
                """)
        
        with col2:
            # Conversion Rate Quality Analysis
            if len(yearly_stats) >= 2:
                st.metric("Last Year Conversion Rate", f"{previous_year['Conversion_Rate']:.1f}%")
                st.metric("Current Year Conversion Rate", f"{current_year['Conversion_Rate']:.1f}%", 
                         delta=f"{conversion_change:.1f}%")
                
                # Intelligent status determination
                if conversion_change > 0 and current_year['Conversion_Rate'] > 25:
                    st.success("✅ Excellent placement efficiency - strong student preparation evident")
                elif conversion_change > 0:
                    st.success("✅ Conversion rate improved - quality focus paying off")
                elif current_year['Conversion_Rate'] > 30:
                    st.info("📊 Maintaining high conversion standards")
                else:
                    st.warning("⚠️ Focus on conversion quality recommended")
        
        # Professional insight summary
        if len(yearly_stats) >= 2:
            st.markdown("---")
            
            # Smart insight generation
            if companies_change > 0 and conversion_change > 0:
                insight_message = f"🎯 **Quality Improvement**: Despite {companies_change} more companies visiting, conversion rate improved by {conversion_change:.1f}%, demonstrating enhanced student preparation and job market alignment."
                st.success(insight_message)
            elif companies_change > 0 and opportunities_per_company_current < opportunities_per_company_previous:
                insight_message = f"📈 **Market Adaptation**: More companies engaged ({companies_change:+}) but market tightening reduced opportunities per company. Focus on conversion efficiency is key."
                st.info(insight_message)
            elif current_year['Conversion_Rate'] > 25:
                st.success("🎯 **Strong Performance**: Maintaining excellent conversion rates indicates effective student preparation strategies.")
    
    # Overall AI Impact Analysis
    st.header("🎯 Overall AI Initiatives Impact")
    
    # Placement success by AI tool usage
    col1, col2 = st.columns(2)
    
    with col1:
        # AI tool usage impact on placement
        placement_by_ai_usage = ai_impact_filtered.groupby(['AI_Tutor_Usage', 'Placed_Not_Placed']).size().unstack(fill_value=0)
        placement_by_ai_usage['Total'] = placement_by_ai_usage.sum(axis=1)
        placement_by_ai_usage['Placement_Rate'] = (placement_by_ai_usage['Placed'] / placement_by_ai_usage['Total'] * 100).round(1)
        
        fig = px.bar(placement_by_ai_usage.reset_index(), x='AI_Tutor_Usage', y='Placement_Rate',
                    title='Placement Rate by AI Tutor Usage Level',
                    labels={'Placement_Rate': 'Placement Rate (%)'})
        fig.update_layout(height=400)
        st.plotly_chart(fig, use_container_width=True)
    
    with col2:
        # CGPA vs AI tool usage
        cgpa_by_ai_usage = ai_impact_filtered.groupby('AI_Tutor_Usage')['CGPA'].mean().reset_index()
        fig = px.bar(cgpa_by_ai_usage, x='AI_Tutor_Usage', y='CGPA',
                    title='Average CGPA by AI Tutor Usage Level',
                    labels={'CGPA': 'Average CGPA'})
        fig.update_layout(height=400)
        st.plotly_chart(fig, use_container_width=True)
    
    # Traditional vs AI Implementation Comparison
    st.subheader("🔄 Traditional Approach vs AI Implementation Comparison")
    
    # Simulate traditional vs AI performance data
    comparison_data = pd.DataFrame({
        'Metric': ['Student Engagement', 'Learning Outcomes', 'Faculty Efficiency', 'Placement Success', 'Student Satisfaction'],
        'Traditional': [65, 70, 60, 75, 68],
        'AI_Enhanced': [85, 88, 82, 89, 87],
        'Improvement': [20, 18, 22, 14, 19]
    })
    
    fig = px.bar(comparison_data, x='Metric', y=['Traditional', 'AI_Enhanced'],
                title='Traditional vs AI-Enhanced Performance Comparison',
                barmode='group')
    fig.update_layout(height=500)
    st.plotly_chart(fig, use_container_width=True)
    
    # Unit Performance Analysis
    st.header("📈 Unit-wise Performance Analysis")
    
    if 'Year' in unit_performance_filtered.columns:
        unit_performance_by_year = unit_performance_filtered.groupby(['Year', 'Unit_Name'])['Total_score'].mean().reset_index()
        
        fig = px.line(unit_performance_by_year, x='Year', y='Total_score', color='Unit_Name',
                    title='Unit Performance Trends Over Years',
                    markers=True)
        fig.update_layout(height=500)
        st.plotly_chart(fig, use_container_width=True)
    
    # Data summary
    st.header("📋 Data Summary")
    
    col1, col2, col3 = st.columns(3)
    
    with col1:
        st.metric("Total Records Analyzed", f"{len(ai_impact_filtered):,}")
    
    with col2:
        st.metric("Total AI Tutor Sessions", f"{len(ai_tutor_filtered):,}")
    
    with col3:
        st.metric("Total JPT Placements", f"{jpt_filtered['Students_Selected'].sum():,}")
    
    # Footer
    st.markdown("---")
    st.markdown("""
    <div style='text-align: center; color: #666;'>
        <p>🚀 AI Initiatives Dashboard | SP Jain School of Global Management</p>
        <p>Data covers MGB & GMBA programs across SG, DXB, and MUM campuses</p>
    </div>
    """, unsafe_allow_html=True)

if __name__ == "__main__":
    main()
