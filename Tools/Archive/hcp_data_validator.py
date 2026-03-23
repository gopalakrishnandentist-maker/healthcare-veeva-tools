"""
HCP Data Validator Tool
Author: Built for OpenData India Operations at Veeva Systems
Purpose: Validate HCP License, Candidate, and Affiliation data at scale
"""

import pandas as pd
import tkinter as tk
from tkinter import ttk, filedialog, messagebox, scrolledtext
import threading
from datetime import datetime
import os

class HCPDataValidator:
    def __init__(self, root):
        self.root = root
        self.root.title("HCP Data Validator - OpenData India | Veeva Systems")
        self.root.geometry("1400x900")
        self.root.configure(bg='#1e1e1e')  # Dark background
        
        self.df = None
        self.results = {}
        self.file_path = None
        
        self.setup_ui()
        
    def setup_ui(self):
        # Color Scheme - Dark Mode Friendly
        bg_dark = '#1e1e1e'          # Main background
        bg_medium = '#2d2d2d'        # Frame backgrounds
        bg_light = '#3a3a3a'         # Input backgrounds
        text_primary = '#e0e0e0'     # Main text
        text_secondary = '#b0b0b0'   # Secondary text
        accent_blue = '#0d7377'      # Buttons
        accent_green = '#14a76c'     # Success
        accent_red = '#ff652f'       # Export/Warning
        accent_orange = '#ff9800'    # Highlights
        
        self.root.configure(bg=bg_dark)
        
        # Header Frame
        header_frame = tk.Frame(self.root, bg='#0d3b66', height=80)
        header_frame.pack(fill='x', padx=0, pady=0)
        header_frame.pack_propagate(False)
        
        title_label = tk.Label(
            header_frame, 
            text="HCP Data Validator", 
            font=('Arial', 24, 'bold'),
            bg='#0d3b66',
            fg='#ffffff'
        )
        title_label.pack(pady=10)
        
        subtitle_label = tk.Label(
            header_frame,
            text="OpenData India Operations | Veeva Systems",
            font=('Arial', 10),
            bg='#0d3b66',
            fg='#a8dadc'
        )
        subtitle_label.pack()
        
        # File Selection Frame
        file_frame = tk.LabelFrame(
            self.root, 
            text="1. Load Data File", 
            font=('Arial', 12, 'bold'),
            bg=bg_medium,
            fg=text_primary,
            padx=10,
            pady=10
        )
        file_frame.pack(fill='x', padx=20, pady=10)
        
        self.file_label = tk.Label(
            file_frame, 
            text="No file selected", 
            font=('Arial', 10),
            bg=bg_medium,
            fg=text_secondary
        )
        self.file_label.pack(side='left', padx=10)
        
        browse_btn = tk.Button(
            file_frame,
            text="Browse Excel File",
            command=self.load_file,
            font=('Arial', 10, 'bold'),
            bg=accent_blue,
            fg='white',
            activebackground='#0a5f63',
            activeforeground='white',
            padx=20,
            pady=5,
            cursor='hand2',
            relief='flat',
            bd=0
        )
        browse_btn.pack(side='right', padx=10)
        
        self.row_count_label = tk.Label(
            file_frame,
            text="",
            font=('Arial', 10, 'bold'),
            bg=bg_medium,
            fg=accent_green
        )
        self.row_count_label.pack(side='right', padx=10)
        
        # Validation Options Frame
        options_frame = tk.LabelFrame(
            self.root,
            text="2. Select Validation Checks",
            font=('Arial', 12, 'bold'),
            bg=bg_medium,
            fg=text_primary,
            padx=10,
            pady=10
        )
        options_frame.pack(fill='x', padx=20, pady=10)
        
        self.check_license = tk.BooleanVar(value=True)
        self.check_candidate = tk.BooleanVar(value=True)
        self.check_affiliation = tk.BooleanVar(value=True)
        
        license_cb = tk.Checkbutton(
            options_frame,
            text="✓ License Check (VIDs without active licenses)",
            variable=self.check_license,
            font=('Arial', 10, 'bold'),
            bg=bg_medium,
            fg=text_primary,
            selectcolor=bg_light,
            activebackground=bg_medium,
            activeforeground=text_primary
        )
        license_cb.grid(row=0, column=0, sticky='w', padx=10, pady=5)
        
        tk.Label(
            options_frame,
            text="   → Checks: license_status__v = Inactive OR license_number__v is blank/0",
            font=('Arial', 9, 'italic'),
            bg=bg_medium,
            fg=text_secondary
        ).grid(row=1, column=0, sticky='w', padx=30, pady=0)
        
        candidate_cb = tk.Checkbutton(
            options_frame,
            text="✓ Candidate Check (Records marked as Candidate)",
            variable=self.check_candidate,
            font=('Arial', 10, 'bold'),
            bg=bg_medium,
            fg=text_primary,
            selectcolor=bg_light,
            activebackground=bg_medium,
            activeforeground=text_primary
        )
        candidate_cb.grid(row=2, column=0, sticky='w', padx=10, pady=5)
        
        tk.Label(
            options_frame,
            text="   → Checks: hcp.candidate_record__v = True",
            font=('Arial', 9, 'italic'),
            bg=bg_medium,
            fg=text_secondary
        ).grid(row=3, column=0, sticky='w', padx=30, pady=0)
        
        affiliation_cb = tk.Checkbutton(
            options_frame,
            text="✓ Active Affiliation Check (VIDs without active HCO affiliation)",
            variable=self.check_affiliation,
            font=('Arial', 10, 'bold'),
            bg=bg_medium,
            fg=text_primary,
            selectcolor=bg_light,
            activebackground=bg_medium,
            activeforeground=text_primary
        )
        affiliation_cb.grid(row=4, column=0, sticky='w', padx=10, pady=5)
        
        tk.Label(
            options_frame,
            text="   → Checks: parent_hco_vid__v is empty OR parent_hco_status__v = Inactive",
            font=('Arial', 9, 'italic'),
            bg=bg_medium,
            fg=text_secondary
        ).grid(row=5, column=0, sticky='w', padx=30, pady=0)
        
        # Run Analysis Button
        button_frame = tk.Frame(self.root, bg=bg_dark)
        button_frame.pack(fill='x', padx=20, pady=15)
        
        self.run_btn = tk.Button(
            button_frame,
            text="▶ Run Analysis",
            command=self.run_analysis,
            font=('Arial', 14, 'bold'),
            bg=accent_green,
            fg='white',
            activebackground='#0f8c56',
            activeforeground='white',
            padx=40,
            pady=10,
            cursor='hand2',
            relief='flat',
            bd=0,
            state='disabled',
            disabledforeground='#666666'
        )
        self.run_btn.pack()
        
        # Progress Bar
        style = ttk.Style()
        style.theme_use('default')
        style.configure("TProgressbar", 
                       background=accent_orange,
                       troughcolor=bg_light,
                       bordercolor=bg_medium,
                       lightcolor=accent_orange,
                       darkcolor=accent_orange)
        
        self.progress = ttk.Progressbar(
            self.root,
            mode='indeterminate',
            length=300,
            style="TProgressbar"
        )
        self.progress.pack(pady=5)
        
        self.status_label = tk.Label(
            self.root,
            text="",
            font=('Arial', 9),
            bg=bg_dark,
            fg=text_secondary
        )
        self.status_label.pack()
        
        # Results Frame with Notebook (Tabs)
        results_frame = tk.LabelFrame(
            self.root,
            text="3. Validation Results",
            font=('Arial', 12, 'bold'),
            bg=bg_medium,
            fg=text_primary,
            padx=10,
            pady=10
        )
        results_frame.pack(fill='both', expand=True, padx=20, pady=10)
        
        # Style for Notebook
        style.configure("TNotebook", background=bg_medium, borderwidth=0)
        style.configure("TNotebook.Tab", 
                       background=bg_light, 
                       foreground=text_primary,
                       padding=[20, 10],
                       font=('Arial', 10, 'bold'))
        style.map("TNotebook.Tab",
                 background=[("selected", accent_blue)],
                 foreground=[("selected", "white")])
        
        self.notebook = ttk.Notebook(results_frame)
        self.notebook.pack(fill='both', expand=True)
        
        # Summary Tab
        self.summary_tab = tk.Frame(self.notebook, bg=bg_light)
        self.notebook.add(self.summary_tab, text='📊 Summary')
        
        self.summary_text = scrolledtext.ScrolledText(
            self.summary_tab,
            font=('Courier New', 10),
            bg='#252525',
            fg='#e0e0e0',
            insertbackground='white',
            wrap='word'
        )
        self.summary_text.pack(fill='both', expand=True, padx=10, pady=10)
        
        # License Issues Tab
        self.license_tab = tk.Frame(self.notebook, bg=bg_light)
        self.notebook.add(self.license_tab, text='📋 License Issues')
        
        self.license_text = scrolledtext.ScrolledText(
            self.license_tab,
            font=('Courier New', 9),
            bg='#252525',
            fg='#e0e0e0',
            insertbackground='white',
            wrap='none'
        )
        self.license_text.pack(fill='both', expand=True, padx=10, pady=10)
        
        # Candidate Issues Tab
        self.candidate_tab = tk.Frame(self.notebook, bg=bg_light)
        self.notebook.add(self.candidate_tab, text='👤 Candidate Records')
        
        self.candidate_text = scrolledtext.ScrolledText(
            self.candidate_tab,
            font=('Courier New', 9),
            bg='#252525',
            fg='#e0e0e0',
            insertbackground='white',
            wrap='none'
        )
        self.candidate_text.pack(fill='both', expand=True, padx=10, pady=10)
        
        # Affiliation Issues Tab
        self.affiliation_tab = tk.Frame(self.notebook, bg=bg_light)
        self.notebook.add(self.affiliation_tab, text='🏥 Affiliation Issues')
        
        self.affiliation_text = scrolledtext.ScrolledText(
            self.affiliation_tab,
            font=('Courier New', 9),
            bg='#252525',
            fg='#e0e0e0',
            insertbackground='white',
            wrap='none'
        )
        self.affiliation_text.pack(fill='both', expand=True, padx=10, pady=10)
        
        # Export Frame
        export_frame = tk.Frame(self.root, bg=bg_dark)
        export_frame.pack(fill='x', padx=20, pady=10)
        
        self.export_btn = tk.Button(
            export_frame,
            text="📊 Export Results to Excel",
            command=self.export_results,
            font=('Arial', 11, 'bold'),
            bg=accent_red,
            fg='white',
            activebackground='#e6451f',
            activeforeground='white',
            padx=20,
            pady=8,
            cursor='hand2',
            relief='flat',
            bd=0,
            state='disabled',
            disabledforeground='#666666'
        )
        self.export_btn.pack()
        
    def load_file(self):
        file_path = filedialog.askopenfilename(
            title="Select HCP Data File",
            filetypes=[
                ("Excel files", "*.xlsx *.xls"),
                ("CSV files", "*.csv"),
                ("All files", "*.*")
            ]
        )
        
        if file_path:
            self.file_path = file_path
            self.status_label.config(text=f"Loading file: {os.path.basename(file_path)}...", fg='#ff9800')
            self.root.update()
            
            try:
                # Load file based on extension
                if file_path.endswith('.csv'):
                    self.df = pd.read_csv(file_path)
                else:
                    self.df = pd.read_excel(file_path, engine='openpyxl')
                
                # Update UI
                self.file_label.config(
                    text=f"✓ {os.path.basename(file_path)}",
                    fg='#14a76c'  # Green for success
                )
                self.row_count_label.config(
                    text=f"{len(self.df):,} rows | {len(self.df.columns)} columns",
                    fg='#14a76c'  # Green for success
                )
                self.run_btn.config(state='normal', bg='#14a76c')
                self.status_label.config(text="File loaded successfully!", fg='#14a76c')
                
                # Show column info
                messagebox.showinfo(
                    "File Loaded",
                    f"Successfully loaded file with:\n\n"
                    f"• Rows: {len(self.df):,}\n"
                    f"• Columns: {len(self.df.columns)}\n\n"
                    f"Ready to run validation checks."
                )
                
            except Exception as e:
                messagebox.showerror("Error", f"Failed to load file:\n{str(e)}")
                self.status_label.config(text="Error loading file", fg='#ff652f')
    
    def run_analysis(self):
        if self.df is None:
            messagebox.showwarning("No Data", "Please load a file first!")
            return
        
        # Check if at least one check is selected
        if not (self.check_license.get() or self.check_candidate.get() or self.check_affiliation.get()):
            messagebox.showwarning("No Checks Selected", "Please select at least one validation check!")
            return
        
        # Disable button and start progress
        self.run_btn.config(state='disabled', bg='#555555')
        self.export_btn.config(state='disabled', bg='#555555')
        self.progress.start(10)
        self.status_label.config(text="Running validation checks...", fg='#ff9800')
        
        # Run in separate thread to keep UI responsive
        thread = threading.Thread(target=self.perform_analysis)
        thread.start()
    
    def perform_analysis(self):
        try:
            self.results = {}
            
            # 1. LICENSE CHECK
            if self.check_license.get():
                self.root.after(0, lambda: self.status_label.config(text="Checking licenses...", fg='#ff9800'))
                license_results = self.check_licenses()
                self.results['license'] = license_results
            
            # 2. CANDIDATE CHECK
            if self.check_candidate.get():
                self.root.after(0, lambda: self.status_label.config(text="Checking candidate records...", fg='#ff9800'))
                candidate_results = self.check_candidates()
                self.results['candidate'] = candidate_results
            
            # 3. AFFILIATION CHECK
            if self.check_affiliation.get():
                self.root.after(0, lambda: self.status_label.config(text="Checking affiliations...", fg='#ff9800'))
                affiliation_results = self.check_affiliations()
                self.results['affiliation'] = affiliation_results
            
            # Display results
            self.root.after(0, self.display_results)
            
        except Exception as e:
            self.root.after(0, lambda: messagebox.showerror("Analysis Error", f"Error during analysis:\n{str(e)}"))
        finally:
            self.root.after(0, self.analysis_complete)
    
    def check_licenses(self):
        """Check for VIDs without active licenses"""
        result = {}
        
        # Find required columns (exact match based on user's column list)
        vid_col = 'hcp.vid__v (VID)'
        license_num_col = 'license.license_number__v (LICENSE)'
        license_status_col = 'license.license_status__v (LICENSE STATUS)'
        first_name_col = 'hcp.first_name__v (FIRST NAME)'
        last_name_col = 'hcp.last_name__v (LAST NAME)'
        
        # Verify columns exist
        missing_cols = []
        for col in [vid_col, license_status_col]:
            if col not in self.df.columns:
                missing_cols.append(col)
        
        if missing_cols:
            result['error'] = f"Required columns not found: {', '.join(missing_cols)}"
            return result
        
        # Create working dataframe
        work_cols = [vid_col, license_status_col]
        if license_num_col in self.df.columns:
            work_cols.append(license_num_col)
        if first_name_col in self.df.columns:
            work_cols.append(first_name_col)
        if last_name_col in self.df.columns:
            work_cols.append(last_name_col)
        
        df_work = self.df[work_cols].copy()
        
        # Determine if license is active
        # Active means: status = 'Active' AND license number is not blank/0
        df_work['is_active'] = (df_work[license_status_col] == 'Active').astype(int)
        
        if license_num_col in self.df.columns:
            # Also check if license number is blank or 0
            df_work['has_license_num'] = ~(
                df_work[license_num_col].isna() | 
                (df_work[license_num_col] == '') |
                (df_work[license_num_col] == '0') |
                (df_work[license_num_col] == 0)
            )
            df_work['is_active'] = (df_work['is_active'] & df_work['has_license_num']).astype(int)
        
        # Count active licenses per VID
        vid_license_count = df_work.groupby(vid_col).agg({
            'is_active': 'sum'
        }).reset_index()
        vid_license_count.columns = ['VID', 'Count_of_Active_Licenses']
        
        # Get name information for VIDs with no active licenses
        vids_no_active = vid_license_count[vid_license_count['Count_of_Active_Licenses'] == 0]['VID'].tolist()
        
        # Create detailed report with names
        if first_name_col in self.df.columns and last_name_col in self.df.columns:
            vid_names = self.df[[vid_col, first_name_col, last_name_col]].drop_duplicates(subset=[vid_col])
            vid_names.columns = ['VID', 'First_Name', 'Last_Name']
            
            details_with_names = vid_license_count.merge(vid_names, on='VID', how='left')
            details_no_active = details_with_names[details_with_names['Count_of_Active_Licenses'] == 0]
        else:
            details_no_active = vid_license_count[vid_license_count['Count_of_Active_Licenses'] == 0]
        
        result['total_vids'] = len(vid_license_count)
        result['vids_no_active'] = len(vids_no_active)
        result['vids_with_active'] = len(vid_license_count) - len(vids_no_active)
        result['details'] = details_no_active
        result['all_counts'] = vid_license_count.sort_values('Count_of_Active_Licenses')
        
        return result
    
    def check_candidates(self):
        """Check for candidate records"""
        result = {}
        
        # Find required columns
        vid_col = 'hcp.vid__v (VID)'
        candidate_col = 'hcp.candidate_record__v (CANDIDATE RECORD)'
        first_name_col = 'hcp.first_name__v (FIRST NAME)'
        last_name_col = 'hcp.last_name__v (LAST NAME)'
        rejection_reason_col = 'hcp.ap_candidate_rejection_reason__c (CANDIDATE REVIEW RESULT)'
        
        # Verify columns exist
        if vid_col not in self.df.columns or candidate_col not in self.df.columns:
            result['error'] = f"Required columns not found"
            return result
        
        # Filter candidate records
        # Handle various representations of True
        candidates = self.df[
            (self.df[candidate_col] == True) | 
            (self.df[candidate_col] == 'True') | 
            (self.df[candidate_col] == 'TRUE') |
            (self.df[candidate_col] == 1) |
            (self.df[candidate_col] == '1') |
            (self.df[candidate_col].astype(str).str.upper() == 'TRUE')
        ].copy()
        
        # Create summary with relevant columns
        cols_to_show = [vid_col]
        if first_name_col in self.df.columns:
            cols_to_show.append(first_name_col)
        if last_name_col in self.df.columns:
            cols_to_show.append(last_name_col)
        cols_to_show.append(candidate_col)
        if rejection_reason_col in self.df.columns:
            cols_to_show.append(rejection_reason_col)
        
        candidate_summary = candidates[cols_to_show].drop_duplicates(subset=[vid_col])
        
        result['total_candidates'] = len(candidate_summary)
        result['details'] = candidate_summary
        
        return result
    
    def check_affiliations(self):
        """Check for VIDs without active HCO affiliations"""
        result = {}
        
        # Find required columns
        vid_col = 'hcp.vid__v (VID)'
        parent_hco_vid_col = 'hco.parent_hco_vid__v (PARENT_HCO_VID__V)'
        parent_status_col = 'hco.parent_hco_status__v (PARENT_HCO_STATUS__V)'
        parent_hco_name_col = 'hco.parent_hco_name__v (PARENT_HCO_NAME__V)'
        first_name_col = 'hcp.first_name__v (FIRST NAME)'
        last_name_col = 'hcp.last_name__v (LAST NAME)'
        
        # Verify columns exist
        if vid_col not in self.df.columns:
            result['error'] = "VID column not found"
            return result
        
        # Create working dataframe
        work_cols = [vid_col]
        if parent_hco_vid_col in self.df.columns:
            work_cols.append(parent_hco_vid_col)
        if parent_status_col in self.df.columns:
            work_cols.append(parent_status_col)
        
        df_work = self.df[work_cols].copy()
        
        # Determine if affiliation is active
        # Active affiliation means: parent_hco_vid is NOT empty AND status is NOT Inactive
        df_work['has_active_affiliation'] = True
        
        if parent_hco_vid_col in self.df.columns:
            df_work['parent_hco_empty'] = (
                self.df[parent_hco_vid_col].isna() | 
                (self.df[parent_hco_vid_col] == '') |
                (self.df[parent_hco_vid_col].astype(str).str.strip() == '')
            )
            df_work['has_active_affiliation'] = df_work['has_active_affiliation'] & ~df_work['parent_hco_empty']
        
        if parent_status_col in self.df.columns:
            df_work['status_inactive'] = (self.df[parent_status_col] == 'Inactive')
            df_work['has_active_affiliation'] = df_work['has_active_affiliation'] & ~df_work['status_inactive']
        
        # Convert to int for counting
        df_work['active_aff_count'] = df_work['has_active_affiliation'].astype(int)
        
        # Group by VID
        vid_affiliation_count = df_work.groupby(vid_col).agg({
            'active_aff_count': 'sum'
        }).reset_index()
        vid_affiliation_count.columns = ['VID', 'Count_of_Active_Affiliations']
        
        # Get VIDs with no active affiliations and add name information
        vids_no_active_aff = vid_affiliation_count[vid_affiliation_count['Count_of_Active_Affiliations'] == 0]
        
        if first_name_col in self.df.columns and last_name_col in self.df.columns:
            vid_names = self.df[[vid_col, first_name_col, last_name_col]].drop_duplicates(subset=[vid_col])
            vid_names.columns = ['VID', 'First_Name', 'Last_Name']
            
            vids_no_active_aff = vids_no_active_aff.merge(vid_names, on='VID', how='left')
        
        result['total_vids'] = len(vid_affiliation_count)
        result['vids_no_active_aff'] = len(vids_no_active_aff)
        result['vids_with_active_aff'] = len(vid_affiliation_count) - len(vids_no_active_aff)
        result['details'] = vids_no_active_aff
        result['all_counts'] = vid_affiliation_count.sort_values('Count_of_Active_Affiliations')
        
        return result
    
    def display_results(self):
        """Display results in the GUI"""
        # Clear existing text
        self.summary_text.delete('1.0', tk.END)
        self.license_text.delete('1.0', tk.END)
        self.candidate_text.delete('1.0', tk.END)
        self.affiliation_text.delete('1.0', tk.END)
        
        # ============ SUMMARY TAB ============
        summary = []
        summary.append("="*90)
        summary.append("HCP DATA VALIDATION REPORT".center(90))
        summary.append("OpenData India Operations | Veeva Systems".center(90))
        summary.append("="*90)
        summary.append(f"\nGenerated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        summary.append(f"Total Records: {len(self.df):,}")
        summary.append(f"File: {os.path.basename(self.file_path)}")
        summary.append("\n" + "="*90)
        
        # License Check Summary
        if 'license' in self.results:
            res = self.results['license']
            summary.append("\n📋 LICENSE CHECK RESULTS:")
            summary.append("-" * 90)
            if 'error' in res:
                summary.append(f"   ❌ ERROR: {res['error']}")
            else:
                summary.append(f"   Total Unique VIDs: {res['total_vids']:,}")
                summary.append(f"   VIDs with Active License(s): {res['vids_with_active']:,}")
                summary.append(f"   ⚠️  VIDs WITHOUT Active License: {res['vids_no_active']:,}")
                if res['total_vids'] > 0:
                    pct = (res['vids_no_active'] / res['total_vids'] * 100)
                    summary.append(f"   Issue Rate: {pct:.2f}%")
                    
                    if pct > 10:
                        summary.append(f"\n   ⚠️  WARNING: High percentage of VIDs without active licenses!")
        
        # Candidate Check Summary
        if 'candidate' in self.results:
            res = self.results['candidate']
            summary.append("\n\n👤 CANDIDATE RECORD CHECK RESULTS:")
            summary.append("-" * 90)
            if 'error' in res:
                summary.append(f"   ❌ ERROR: {res['error']}")
            else:
                summary.append(f"   ⚠️  Records Marked as Candidate: {res['total_candidates']:,}")
                if res['total_candidates'] > 0:
                    summary.append(f"\n   ℹ️  These records require review and validation")
        
        # Affiliation Check Summary
        if 'affiliation' in self.results:
            res = self.results['affiliation']
            summary.append("\n\n🏥 AFFILIATION CHECK RESULTS:")
            summary.append("-" * 90)
            if 'error' in res:
                summary.append(f"   ❌ ERROR: {res['error']}")
            else:
                summary.append(f"   Total Unique VIDs: {res['total_vids']:,}")
                summary.append(f"   VIDs with Active Affiliation(s): {res['vids_with_active_aff']:,}")
                summary.append(f"   ⚠️  VIDs WITHOUT Active Affiliation: {res['vids_no_active_aff']:,}")
                if res['total_vids'] > 0:
                    pct = (res['vids_no_active_aff'] / res['total_vids'] * 100)
                    summary.append(f"   Issue Rate: {pct:.2f}%")
                    
                    if pct > 15:
                        summary.append(f"\n   ⚠️  WARNING: High percentage of VIDs without active affiliations!")
        
        summary.append("\n" + "="*90)
        summary.append("\nNext Steps:")
        summary.append("  1. Review detailed results in individual tabs")
        summary.append("  2. Export results to Excel for further analysis")
        summary.append("  3. Share findings with data stewards and stakeholders")
        summary.append("="*90)
        
        self.summary_text.insert('1.0', '\n'.join(summary))
        
        # ============ LICENSE TAB ============
        if 'license' in self.results:
            res = self.results['license']
            if 'error' not in res and res['vids_no_active'] > 0:
                license_output = []
                license_output.append("VIDs WITHOUT Active Licenses")
                license_output.append("="*90)
                license_output.append(f"Total: {res['vids_no_active']:,} VIDs\n")
                
                # Display details
                df_display = res['details'].copy()
                license_output.append(df_display.to_string(index=False))
                
                self.license_text.insert('1.0', '\n'.join(license_output))
            elif 'error' not in res:
                self.license_text.insert('1.0', "✓ All VIDs have at least one active license!")
            else:
                self.license_text.insert('1.0', f"Error: {res['error']}")
        
        # ============ CANDIDATE TAB ============
        if 'candidate' in self.results:
            res = self.results['candidate']
            if 'error' not in res and res['total_candidates'] > 0:
                candidate_output = []
                candidate_output.append("CANDIDATE RECORDS")
                candidate_output.append("="*90)
                candidate_output.append(f"Total: {res['total_candidates']:,} Candidate Records\n")
                candidate_output.append("These records need review and validation:\n")
                
                # Display details
                df_display = res['details'].copy()
                candidate_output.append(df_display.to_string(index=False))
                
                self.candidate_text.insert('1.0', '\n'.join(candidate_output))
            elif 'error' not in res:
                self.candidate_text.insert('1.0', "✓ No candidate records found!")
            else:
                self.candidate_text.insert('1.0', f"Error: {res['error']}")
        
        # ============ AFFILIATION TAB ============
        if 'affiliation' in self.results:
            res = self.results['affiliation']
            if 'error' not in res and res['vids_no_active_aff'] > 0:
                affiliation_output = []
                affiliation_output.append("VIDs WITHOUT Active HCO Affiliations")
                affiliation_output.append("="*90)
                affiliation_output.append(f"Total: {res['vids_no_active_aff']:,} VIDs\n")
                
                # Display details
                df_display = res['details'].copy()
                affiliation_output.append(df_display.to_string(index=False))
                
                self.affiliation_text.insert('1.0', '\n'.join(affiliation_output))
            elif 'error' not in res:
                self.affiliation_text.insert('1.0', "✓ All VIDs have at least one active affiliation!")
            else:
                self.affiliation_text.insert('1.0', f"Error: {res['error']}")
    
    def analysis_complete(self):
        """Called when analysis is complete"""
        self.progress.stop()
        self.run_btn.config(state='normal', bg='#14a76c')
        self.export_btn.config(state='normal', bg='#ff652f')
        self.status_label.config(text="✓ Analysis complete!", fg='#14a76c')
        
        messagebox.showinfo(
            "Analysis Complete",
            "Validation checks completed successfully!\n\n"
            "Review results in the tabs above and export to Excel if needed."
        )
    
    def export_results(self):
        """Export results to Excel"""
        if not self.results:
            messagebox.showwarning("No Results", "Please run analysis first!")
            return
        
        # Ask for save location
        file_path = filedialog.asksaveasfilename(
            defaultextension=".xlsx",
            filetypes=[("Excel files", "*.xlsx")],
            initialfile=f"HCP_Validation_Results_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"
        )
        
        if not file_path:
            return
        
        try:
            with pd.ExcelWriter(file_path, engine='openpyxl') as writer:
                
                # Summary Sheet
                summary_data = {
                    'Metric': [],
                    'Value': []
                }
                
                summary_data['Metric'].append('File Analyzed')
                summary_data['Value'].append(os.path.basename(self.file_path))
                
                summary_data['Metric'].append('Total Records')
                summary_data['Value'].append(len(self.df))
                
                summary_data['Metric'].append('Analysis Date')
                summary_data['Value'].append(datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
                
                summary_data['Metric'].append('')
                summary_data['Value'].append('')
                
                # License metrics
                if 'license' in self.results and 'error' not in self.results['license']:
                    res = self.results['license']
                    summary_data['Metric'].extend([
                        'LICENSE CHECK',
                        'Total VIDs',
                        'VIDs with Active License',
                        'VIDs WITHOUT Active License',
                        'License Issue Rate (%)'
                    ])
                    summary_data['Value'].extend([
                        '',
                        res['total_vids'],
                        res['vids_with_active'],
                        res['vids_no_active'],
                        f"{(res['vids_no_active'] / res['total_vids'] * 100):.2f}%" if res['total_vids'] > 0 else "0%"
                    ])
                    
                    summary_data['Metric'].append('')
                    summary_data['Value'].append('')
                
                # Candidate metrics
                if 'candidate' in self.results and 'error' not in self.results['candidate']:
                    res = self.results['candidate']
                    summary_data['Metric'].extend([
                        'CANDIDATE CHECK',
                        'Candidate Records Found'
                    ])
                    summary_data['Value'].extend([
                        '',
                        res['total_candidates']
                    ])
                    
                    summary_data['Metric'].append('')
                    summary_data['Value'].append('')
                
                # Affiliation metrics
                if 'affiliation' in self.results and 'error' not in self.results['affiliation']:
                    res = self.results['affiliation']
                    summary_data['Metric'].extend([
                        'AFFILIATION CHECK',
                        'Total VIDs',
                        'VIDs with Active Affiliation',
                        'VIDs WITHOUT Active Affiliation',
                        'Affiliation Issue Rate (%)'
                    ])
                    summary_data['Value'].extend([
                        '',
                        res['total_vids'],
                        res['vids_with_active_aff'],
                        res['vids_no_active_aff'],
                        f"{(res['vids_no_active_aff'] / res['total_vids'] * 100):.2f}%" if res['total_vids'] > 0 else "0%"
                    ])
                
                pd.DataFrame(summary_data).to_excel(writer, sheet_name='Summary', index=False)
                
                # License Issues Sheet
                if 'license' in self.results and 'error' not in self.results['license']:
                    if len(self.results['license']['details']) > 0:
                        self.results['license']['details'].to_excel(
                            writer, sheet_name='License Issues', index=False
                        )
                    
                    # Also export all VID license counts
                    self.results['license']['all_counts'].to_excel(
                        writer, sheet_name='All VID License Counts', index=False
                    )
                
                # Candidate Records Sheet
                if 'candidate' in self.results and 'error' not in self.results['candidate']:
                    if len(self.results['candidate']['details']) > 0:
                        self.results['candidate']['details'].to_excel(
                            writer, sheet_name='Candidate Records', index=False
                        )
                
                # Affiliation Issues Sheet
                if 'affiliation' in self.results and 'error' not in self.results['affiliation']:
                    if len(self.results['affiliation']['details']) > 0:
                        self.results['affiliation']['details'].to_excel(
                            writer, sheet_name='Affiliation Issues', index=False
                        )
                    
                    # Also export all VID affiliation counts
                    self.results['affiliation']['all_counts'].to_excel(
                        writer, sheet_name='All VID Affiliation Counts', index=False
                    )
            
            messagebox.showinfo(
                "Export Successful",
                f"Results exported successfully to:\n\n{file_path}"
            )
            
            # Ask if user wants to open the file
            if messagebox.askyesno("Open File", "Would you like to open the exported file?"):
                import subprocess
                import platform
                
                if platform.system() == 'Windows':
                    os.startfile(file_path)
                elif platform.system() == 'Darwin':  # macOS
                    subprocess.call(['open', file_path])
                else:  # Linux
                    subprocess.call(['xdg-open', file_path])
        
        except Exception as e:
            messagebox.showerror("Export Error", f"Failed to export results:\n{str(e)}")


def main():
    root = tk.Tk()
    app = HCPDataValidator(root)
    root.mainloop()


if __name__ == "__main__":
    main()
