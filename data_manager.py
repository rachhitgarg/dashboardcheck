import pandas as pd
import os
import logging
from datetime import datetime
import streamlit as st
import zipfile
from io import BytesIO

# Configure logging
logging.basicConfig(
    filename='data_operations.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

class DataManager:
    """Class to handle data upload, download, merge, and delete operations"""
    
    def __init__(self):
        self.data_files = {
            'AI Tutor': 'ai_tutor_mock_data.csv',
            'AI Mentor': 'ai_mentor_mock_data.csv',
            'AI Impact': 'ai_impact_mock_data.csv',
            'JPT Data': 'jpt_mock_data.csv',
            'Unit Performance': 'unit_performance_mock_data.csv'
        }
        
        self.templates = {
            'AI Tutor': {
                'filename': 'ai_tutor_template.csv',
                'columns': ['Campus', 'Course_Name', 'Cohort', 'Unit_Name', 'Faculty', 'Email_ID',
                          'Unit_Commencement_date', 'No_of_Session_IDs_created', 'Total_Students_Participated',
                          'Batch_size', 'Student_adoption_rate', 'End_Date', 'No_of_students_who_filled_form',
                          'Size_of_batch_when_feedback_taken', 'Faculty_Avg_Score', 'AI_Tutor_quality_score',
                          'AI_Tutor_impact_score', 'Avg_Rating_for_AI_Tutor_Tool', 'Implemented_AI_Tutor',
                          'Features_found_useful', 'Used_Document_Creator', 'Quizzes_conducted',
                          'Quizzes_used_for_grading', 'Quiz_outcome', 'Faculty_Feedback']
            },
            'AI Mentor': {
                'filename': 'ai_mentor_template.csv',
                'columns': ['Academic_Manager_Name', 'Program', 'Cohort', 'Term', 'Q1_Improvement_observed',
                          'Q2_Students_motivated', 'Q3_Effectiveness', 'Q4_Key_observations']
            },
            'AI Impact': {
                'filename': 'ai_impact_template.csv',
                'columns': ['Student_Name', 'Student_mail_id', 'Program', 'Cohort', 'Term',
                          'Placed_Not_Placed', 'CGPA', 'AI_Tutor_Usage', 'AI_Mentor_Usage',
                          'AI_TKT_Exam_Usage', 'JPT_Usage', 'Yoodli_Usage']
            },
            'JPT Data': {
                'filename': 'jpt_template.csv',
                'columns': ['Year', 'Program', 'Cohort', 'Company', 'Industry_Sector', 'Company_Tier',
                          'Job_role', 'Location', 'Students_Eligible', 'Applied_Y_N', 'Students_Interviewed',
                          'Vacancies_Offered', 'Students_Selected', 'Avg_CTC', 'Highest_CTC']
            },
            'Unit Performance': {
                'filename': 'unit_performance_template.csv',
                'columns': ['Unit_Name', 'CP', 'IA', 'GA', 'TE', 'Total_score', 'Year', 'Program', 'Cohort']
            }
        }
    
    def log_operation(self, operation, data_type, user_info, details=""):
        """Log data operations for audit trail"""
        log_message = f"Operation: {operation} | Data Type: {data_type} | User: {user_info} | Details: {details}"
        logging.info(log_message)
        
        # Also create a session log for display
        if 'operation_logs' not in st.session_state:
            st.session_state.operation_logs = []
        
        st.session_state.operation_logs.append({
            'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            'operation': operation,
            'data_type': data_type,
            'user': user_info,
            'details': details
        })
    
    def create_template(self, data_type):
        """Create empty template for data type"""
        if data_type in self.templates:
            template_df = pd.DataFrame(columns=self.templates[data_type]['columns'])
            return template_df
        return None
    
    def download_template(self, data_type):
        """Generate downloadable template"""
        template_df = self.create_template(data_type)
        if template_df is not None:
            csv_buffer = BytesIO()
            template_df.to_csv(csv_buffer, index=False)
            return csv_buffer.getvalue()
        return None
    
    def download_all_templates(self):
        """Create a zip file with all templates"""
        zip_buffer = BytesIO()
        
        with zipfile.ZipFile(zip_buffer, 'w', zipfile.ZIP_DEFLATED) as zip_file:
            for data_type, template_info in self.templates.items():
                template_df = self.create_template(data_type)
                if template_df is not None:
                    csv_buffer = BytesIO()
                    template_df.to_csv(csv_buffer, index=False)
                    zip_file.writestr(template_info['filename'], csv_buffer.getvalue())
        
        return zip_buffer.getvalue()
    
    def validate_uploaded_data(self, uploaded_df, data_type):
        """Validate uploaded data structure"""
        if data_type not in self.templates:
            return False, "Invalid data type"
        
        expected_columns = set(self.templates[data_type]['columns'])
        uploaded_columns = set(uploaded_df.columns)
        
        missing_columns = expected_columns - uploaded_columns
        extra_columns = uploaded_columns - expected_columns
        
        if missing_columns:
            return False, f"Missing columns: {', '.join(missing_columns)}"
        
        if extra_columns:
            st.warning(f"Extra columns found (will be ignored): {', '.join(extra_columns)}")
        
        return True, "Valid data structure"
    
    def load_existing_data(self, data_type):
        """Load existing data file"""
        filename = self.data_files.get(data_type)
        if filename and os.path.exists(filename):
            try:
                return pd.read_csv(filename)
            except Exception as e:
                st.error(f"Error loading existing data: {e}")
                return pd.DataFrame()
        return pd.DataFrame()
    
    def merge_data(self, existing_df, new_df, data_type, user_info):
        """Merge new data with existing data"""
        try:
            # Filter new data to only include expected columns
            expected_columns = self.templates[data_type]['columns']
            new_df_filtered = new_df[expected_columns]
            
            if existing_df.empty:
                merged_df = new_df_filtered
            else:
                merged_df = pd.concat([existing_df, new_df_filtered], ignore_index=True)
            
            # Remove duplicates if any
            merged_df = merged_df.drop_duplicates()
            
            self.log_operation("MERGE", data_type, user_info, 
                             f"Added {len(new_df_filtered)} records, Total: {len(merged_df)}")
            
            return merged_df, True, "Data merged successfully"
            
        except Exception as e:
            return existing_df, False, f"Error merging data: {e}"
    
    def replace_data(self, new_df, data_type, user_info):
        """Replace existing data with new data"""
        try:
            # Filter new data to only include expected columns
            expected_columns = self.templates[data_type]['columns']
            new_df_filtered = new_df[expected_columns]
            
            self.log_operation("REPLACE", data_type, user_info, 
                             f"Replaced all data with {len(new_df_filtered)} new records")
            
            return new_df_filtered, True, "Data replaced successfully"
            
        except Exception as e:
            return pd.DataFrame(), False, f"Error replacing data: {e}"
    
    def save_data(self, df, data_type):
        """Save data to file"""
        filename = self.data_files.get(data_type)
        if filename:
            try:
                df.to_csv(filename, index=False)
                return True, "Data saved successfully"
            except Exception as e:
                return False, f"Error saving data: {e}"
        return False, "Invalid data type"
    
    def delete_data(self, data_type, user_info):
        """Delete all data for a specific type"""
        try:
            filename = self.data_files.get(data_type)
            if filename and os.path.exists(filename):
                # Create backup before deletion
                backup_filename = f"{filename}.backup_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
                df = pd.read_csv(filename)
                df.to_csv(backup_filename, index=False)
                
                # Create empty dataframe with correct structure
                empty_df = self.create_template(data_type)
                empty_df.to_csv(filename, index=False)
                
                self.log_operation("DELETE", data_type, user_info, 
                                 f"All data deleted, backup created: {backup_filename}")
                
                return True, f"Data deleted successfully. Backup created: {backup_filename}"
            else:
                return False, "Data file not found"
                
        except Exception as e:
            return False, f"Error deleting data: {e}"
    
    def get_data_summary(self):
        """Get summary of all data files"""
        summary = {}
        for data_type, filename in self.data_files.items():
            if os.path.exists(filename):
                try:
                    df = pd.read_csv(filename)
                    summary[data_type] = {
                        'records': len(df),
                        'last_modified': datetime.fromtimestamp(os.path.getmtime(filename)).strftime('%Y-%m-%d %H:%M:%S'),
                        'file_size': f"{os.path.getsize(filename) / 1024:.1f} KB"
                    }
                except Exception as e:
                    summary[data_type] = {'error': str(e)}
            else:
                summary[data_type] = {'records': 0, 'status': 'File not found'}
        
        return summary
