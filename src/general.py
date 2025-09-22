import calendar
import os
import pickle
import random
import re
import subprocess
import sys
import time
import traceback
import warnings
import zipfile
from concurrent.futures import (ProcessPoolExecutor, ThreadPoolExecutor,
                                as_completed)
from datetime import datetime, timedelta
from io import StringIO

import boto3
import pygsheets


application_path = os.path.dirname(sys.executable)
warnings.filterwarnings("ignore", category=UserWarning, module="openpyxl")
now = datetime.now()
day_month_year = now.strftime("%d%m%y")

address = os.path.join(os.getcwd(), "inventory_files")
current_dir = os.getcwd()


# if "/" in os.getcwd():
#     address = os.getcwd() + "/inventory_files"
# else:
#     address = os.getcwd() + "\inventory_files"

# current_dir = os.getcwd()

fk_pop_up_list=list_of_cross=['[aria-label="Skip"]','[title="Skip"]','[title="Close"] svg','.ReactModal__Content--after-open .eQZbrp > div','.HYMgb','#app .chlJSj span','#popover-content button',
                 '[data-id="CLOSE"]','.fa-close','.ReactModal__Overlay--after-open .bdJOfF button','.ReactModal__Overlay--after-open .ReactModal__Header__Close','.__floater__body button'
                   ]

def empty_folder(address):
    for file in os.listdir(address):
        if "/" in os.getcwd():
            fileadd = str(
                        address
                        + "/"
                        + os.listdir(address)[0]
                    )
        else:
            fileadd = str(
                        address
                        + "\\"
                        + os.listdir(address)[0]
                    )

        os.remove(fileadd)

def get_gc(index):
    service_file_path = os.path.join(os.getcwd(), f"key{index}.json")
    gc = pygsheets.authorize(service_file=service_file_path)
    return gc

service_file_path = os.path.join(current_dir, "key.json")
gc = pygsheets.authorize(service_file=service_file_path)
index=""
def print_sheet(index,final_result, sheet_name, sheet_id, x, y, mode=1,make_upper_case=0):
    current_time = datetime.now()
    current_date_string = current_time.strftime('%Y-%m-%d')
    if "updated_date" not in final_result.columns:
        final_result['updated_date'] = current_date_string
    index=int(index)
    index=index%12
    # time.sleep(4)
    service_file_path = os.path.join(os.getcwd(), f"key{index}.json")
    print(service_file_path)
    gc = pygsheets.authorize(service_file=service_file_path)
    sh=gc.open_by_key(sheet_id)
    selected_sheet = sh.worksheet_by_title(sheet_name)
    if make_upper_case==1:
        column_names = final_result.columns.tolist()
        converted_columns = []
        for column_name in column_names:
            column_name = column_name.split('_')
            column_name = list(map(lambda x:x.capitalize(), column_name))
            column_name = (' ').join(column_name)
            converted_columns.append(column_name)
        names_mapping = dict(zip(column_names, converted_columns))
        final_result = final_result.rename(columns=names_mapping)

    

    """ 
    Mode 2 will now append data while keeping the headers intact and delete the column names row for the new data.
    Mode 2 adds only 2 new rows
    """
    """
    Mode 3 will also add data while maintaining headers, but will delete the column names row for the new data.
    Mode 3 adds the entire lenght of the new dataframe + 2 rows
    """
    if mode == 1:
        selected_sheet.set_dataframe(final_result, (x, y), fit=True)

    elif mode == 2:
        last_filled_row = len(selected_sheet.get_as_df())
        selected_sheet.add_rows(2)
        print_from = last_filled_row + 2

        if last_filled_row == 0:
            selected_sheet.set_dataframe(final_result, (1, 1))

        else:
            selected_sheet.set_dataframe(final_result, (print_from, 1))
            selected_sheet.delete_rows(print_from)


    elif mode == 3:
        last_filled_row = len(selected_sheet.get_as_df())
        total_rows_needed = last_filled_row + len(final_result) + 2
        existing_rows = selected_sheet.rows

        # Add rows only if needed
        if total_rows_needed > existing_rows:
            selected_sheet.add_rows(total_rows_needed - existing_rows)

        time.sleep(3)
        print_from = last_filled_row + 2 if last_filled_row > 0 else 1
        selected_sheet.set_dataframe(final_result, (print_from, 1))

        if last_filled_row > 0:
            selected_sheet.delete_rows(print_from)
    else:
        selected_sheet.set_dataframe(final_result, (x, y))
    print(f"uploaded * sheet name *{sheet_name}* in sheet_id *{sheet_id}")


def clear_google_sheet(index, sheet_name, sheet_id):
    index=int(index)
    index=index%12
    time.sleep(2)
    service_file_path = os.path.join(os.getcwd(), f"key{index}.json")
    print(service_file_path)
    gc = pygsheets.authorize(service_file=service_file_path)
    sh=gc.open_by_key(sheet_id)
    selected_sheet = sh.worksheet_by_title(sheet_name)
    selected_sheet.clear()

def get_class(driver, text, position=-1, mode=1, nth_child=1):
    # position is 1 indexed
    time.sleep(5)
    page_source = driver.page_source

    soup = BeautifulSoup(page_source, "html.parser")
    # Find the element containing the text "Create report"
    element = soup.find_all(string=text)
    selected_element = element[position]
    parent_element = selected_element.find_parent()
    # class_name = element.parent.get('class')
    parent_class_name = parent_element.get("class")

    parent_class_name_css = "." + ".".join(parent_class_name)
    parent_of_parent = parent_element.findParent()
    parent_of_parent.get("class")[-1]
    parent_of_parent_css = "." + parent_of_parent.get("class")[-1]
    if mode == 1:
        # mode 1 for class name
        return parent_class_name_css
    if mode == 2:
        # mode 2 for parent class
        return parent_of_parent_css
    if mode == 3:
        # mode 3 for combination
        return parent_of_parent_css + " " + parent_class_name_css
    if mode == 4:
        # mode for nth child
        return parent_of_parent_css + f" :nth-child({nth_child})"
    if mode == 5:
        return parent_class_name_css + f" :nth-child({nth_child})"



def read_and_remove(address,skiprows=0, sheet_name=0, run_times=20):
    #   it should not be 100
    print(f"started file reading from {address}")
    print(f"..............{len(os.listdir(address)),os.listdir(address)}")
    for times in range(run_times):
        time.sleep(10)
        try:


            if len(os.listdir(address)) >= 1:
                print(f"file detected at {address}")
                fileadd = os.path.join(address, os.listdir(address)[0])
                _, file_extension = os.path.splitext(fileadd)

                # Read the file based on its extension
                if file_extension in ['.csv']:
                    print("in side csv thing")
                    df=pd.read_csv(fileadd, skiprows=skiprows,index_col=False, on_bad_lines='skip')
                    empty_folder(address=address)
                    return df
                elif file_extension in ['.xls', '.xlsx', '.xlsm']:
                    df=pd.read_excel(fileadd,sheet_name=sheet_name,engine="openpyxl")
                    empty_folder(address=address)
                    return df
                else:
                    print("Not known case of error ")
                empty_folder(address=address)

            else:
                time.sleep(10)
                print("check again")
        except Exception as e:
            print(str(e))
            
            pass
            break
    print(f"file not found at {address}")

def read_and_remove_excel_default(address,skip_rows=0):
    print(address)
    print(f"started file reading from {address}")
    print(f"..............{len(os.listdir(address)),os.listdir(address)}")
    time.sleep(5)
    for times in range(10):
        try:
            fileadd = os.path.join(address, os.listdir(address)[0])
            print(fileadd)
            df = pd.read_excel(fileadd, engine='xlrd',skiprows=skip_rows)
            print("read file")
            time.sleep(10)
            empty_folder(address)
            return df
            break

        except Exception as e:
            print("parser error or html error ")
            pass
            break

def add_sheet_name(sheet_id, sheet_name,index):
    index=int(index)
    index=index%12
    service_file_path = os.path.join(os.getcwd(), f"key{index}.json")
    print(service_file_path)
    gc = pygsheets.authorize(service_file=service_file_path)
    raw_sh =gc.open_by_key(sheet_id)
    all_sheets = raw_sh.worksheets()
    existing_sheet = []
    for each_sheet in all_sheets:
        existing_sheet.append(each_sheet.title)
    if sheet_name not in existing_sheet:
        raw_sh.add_worksheet(sheet_name)
    else:
        pass
    time.sleep(3)


def read_and_remove_pdf(address, max_retries=10):
    print(f"Reading file from {address}")
    for retry in range(0, max_retries):
        files = os.listdir(address)
        if files:
            print(f"Files found - {files}")
            file = files[0]
            if file.endswith(".pdf") or file.endswith(".PDF"):
                print(f"Reading the pdf file {file}")
                file_address = os.path.join(address, file)
                reader = PdfReader(file_address)
                page = reader.pages[0]
                page_text = page.extract_text()
                empty_folder(address)
                return page_text
            else:
                print(f"The file {file} is not a pdf file")
                return None
        else:
            print(f"Retry Number - {retry+1}")
            print("File not downloaded yet. So, waiting for 30 sec")
            time.sleep(2)
    print("File not found after max retries")
    return None


def clear_worksheet(sheet_id, worksheet_names,index):
    index=int(index)
    index=index%12
    service_file_path = os.path.join(os.getcwd(), f"key{index}.json")
    print(service_file_path)
    gc = pygsheets.authorize(service_file=service_file_path)
    raw_sh =gc.open_by_key(sheet_id)
    try:
      worksheet = raw_sh.worksheet_by_title(worksheet_names)
      print(worksheet_names)
      print(worksheet)
      # Clear the contents of the specified worksheet
      worksheet.clear()
    except pygsheets.exceptions.WorksheetNotFound:
        print(f"Worksheet '{worksheet_names}' not found.")
        
    time.sleep(3)

def select_and_click_with_css(driver, css):
    try:
        element = driver.find_element(By.CSS_SELECTOR, css)
        time.sleep(1.5)
        element.click()
        return element
    except NoSuchElementException:
        # print(f'error {css} missing')
        pass


def select_only_no_click_with_css(driver, css):
    try:
        element = driver.find_element(By.CSS_SELECTOR, css)
        return element
    except NoSuchElementException:
        # print(f'error {css} missing')
        pass


def select_and_click_with_text(driver, text):
    try:
        brand_name=driver.find_element(By.XPATH, f"//*[contains(text(), '{text}')]").text
        if brand_name == 'NextechDeals':
            all_element=driver.find_elements(By.XPATH, f"//*[contains(text(), '{text}')]")
            all_element[1].click()
        driver.find_element(By.XPATH, f"//*[contains(text(), '{text}')]").click()
    except:
        # print(f"{text} not found")
        pass


def select_only_no_click_with_text(driver, text):
    try:
        element = driver.find_element(By.XPATH, f"//*[contains(text(), '{text}')]")
        return element
    except:
        # print(f"{text} not found")
        pass


def select_multiple_element_with_css(driver, css):
    try:
        element = driver.find_elements(By.CSS_SELECTOR, css)
        return element
    except NoSuchElementException:
        # print(f'error {css} missing')
        pass


def select_multiple_element_with_text(driver, text):
    try:
        element = driver.find_elements(
            By.XPATH, f"//*[contains(text(), '{text}')]"
        ).click()
        return element
    except:
        # print(f"{text} not found")
        pass


def select_only_with_css_no_exception_handling(driver, css):
    ele = driver.find_element(By.CSS_SELECTOR, css)
    return ele


# def extract_downloaded_file_read_and_remove(driver):
#     time.sleep(5)
#     # getting the fileaddress
#     for file in os.listdir(address):
#         if "/" in os.getcwd():
#             fileadd = str(os.getcwd()) + f"/inventory_files/{file}"
#         else:
#             fileadd = str(os.getcwd()) + f"\inventory_files\{file}"

#     print(fileadd)

#     # getting the extracted file path

#     # extracted_file_path = "c:\\Users\\sohai\\OneDrive\\Desktop\\Blooprint Business\\Business\\extracted_files".

#     for _ in os.listdir():
#         time.sleep(5)
#         if "/" in os.getcwd():
#             extracted_file_path = str(os.getcwd()) + f"//extracted_files//"
#             break
#         else:
#             extracted_file_path = str(os.getcwd()) + f"\\extracted_files\\"
#             break

#     with zipfile.ZipFile(fileadd, "r") as zip_ref:
#         time.sleep(5)
#         zip_ref.extractall(extracted_file_path)

#     time.sleep(2)
#     # List files in the extracted folder
#     extracted_files = os.listdir(extracted_file_path)

#     # Find the CSV file (assuming there's only one CSV file)
#     csv_file = [file for file in extracted_files if file.endswith(".csv")]

#     if not csv_file:
#         print("No CSV file found in the extracted folder.")
#     else:
#         # Assuming there's only one CSV file, you can directly access it
#         csv_file_path = os.path.join(extracted_file_path, csv_file[0])

#     df = pd.read_csv(csv_file_path)
#     time.sleep(2)
#     os.remove(fileadd)
#     os.remove(csv_file_path)
#     return df

def extract_downloaded_file_read_and_remove(address, max_retries=10):
    for retry in range(0, max_retries):
        try:
            zip_files = [file for file in os.listdir(address) if file.endswith(".zip")]
            if not zip_files:
                raise FileNotFoundError("No ZIP file found in the given folder.")

            zip_file_path = os.path.join(address, zip_files[0])
            print(f"Found ZIP file: {zip_file_path}")

            extracted_file_path = os.path.join(os.getcwd(), "extracted_files")
            os.makedirs(extracted_file_path, exist_ok=True)

            with zipfile.ZipFile(zip_file_path, "r") as zip_ref:
                zip_ref.extractall(extracted_file_path)

            extracted_files = os.listdir(extracted_file_path)
            csv_files = [file for file in extracted_files if file.endswith(".csv")]

            if not csv_files:
                raise FileNotFoundError("No CSV file found in the extracted folder.")

            csv_file_path = os.path.join(extracted_file_path, csv_files[0])
            df = pd.read_csv(csv_file_path)

            os.remove(zip_file_path)
            os.remove(csv_file_path)
            return df

        except:
            print(f"Retry Number - {retry+1}")
            print(f"file not found---waiting for file")
            

def send_data_to_sheet(index,account_name,sheet_name,raw_sheet,address):
    index=int(index)
    index=index%12
    print(f"searching file in {address}")
    time.sleep(10)
    current_time = datetime.now()
    current_month = current_time.month

    current_date_string = current_time.strftime('%Y-%m-%d')
    df=read_and_remove(skiprows=0,address=address)
    # try:
    #     print(df.head(5))
    # except UnicodeEncodeError as e:
    #     print(df.head(5).applymap(lambda x: x.encode('utf-8').decode('utf-8') if isinstance(x, str) else x))
    #     print(len(df))
    if df is not None:
        print(f"length of file is {len(df)}")
        df['Account Name'] = account_name
        df["updated_date"] = current_date_string
        add_sheet_name(sheet_id=raw_sheet,sheet_name=sheet_name,index=index)
        if len(df)==0:
            df = database.rename_columns(df)
            print_sheet(index,df,sheet_name,sheet_id=raw_sheet,x=1,y=1)
        print(f"sent to sheet {sheet_name}")
        return df
    else:
        clear_worksheet(raw_sheet,sheet_name,index)
        print("either the df was none or of no length")
        pass

def click_element_in_shadow_dom(driver, shadow_host_css, element_to_click_css):
    shadow_host = driver.find_element(By.CSS_SELECTOR, shadow_host_css)

    # Define JavaScript code to access the Shadow DOM
    script = """
        const shadowRoot = arguments[0].shadowRoot;
        const elementInsideShadowDOM = shadowRoot.querySelector(arguments[1]);
        elementInsideShadowDOM.click();
    """

    # Execute the JavaScript code to click on the element inside the Shadow DOM
    driver.execute_script(script, shadow_host, element_to_click_css)


def get_text_from_shadow_dom(driver, shadow_host_css, element_to_get_text_css):
    shadow_host = driver.find_element(By.CSS_SELECTOR, shadow_host_css)

    # Define JavaScript code to access the Shadow DOM and get text content
    script = """
        const shadowRoot = arguments[0].shadowRoot;
        const elementInsideShadowDOM = shadowRoot.querySelector(arguments[1]);
        return elementInsideShadowDOM.textContent;
    """

    # Execute the JavaScript code to get the text content of the element inside the Shadow DOM
    text_content = driver.execute_script(script, shadow_host, element_to_get_text_css)

    return text_content


def iframe_switch_by_id(driver, id):
    iframe_id = id
    iframe_element = driver.find_element(By.ID, iframe_id)

    # Switch to the iframe
    driver.switch_to.frame(iframe_element)
def iframe_switch_by_css(driver, css):
    iframe_id = css
    iframe_element = driver.find_element(By.CSS_SELECTOR, iframe_id)

    # Switch to the iframe
    driver.switch_to.frame(iframe_element)


def iframe_switch_to_default_content(driver):
    driver.switch_to.default_content()


def get_file_add():
    time.sleep(5)
    if "/" in os.getcwd():
        address = os.getcwd() + "/inventory_files"
    else:
        address = os.getcwd() + "\inventory_files"
    if "/" in os.getcwd():
        fileadd = str(os.getcwd()) + f"/inventory_files/{address}"
    else:
        fileadd = str(os.getcwd()) + f"\inventory_files\{address}"


def read_and_remove_txt(delimiter="\t",address=address):
    time.sleep(10)
    for times in range(20):
        if len(os.listdir(f"{address}")) >= 1:
            if "/" in os.getcwd():
                    fileadd = str(
                        address
                        + "/"
                        + os.listdir(address)[0])
            else:
                fileadd = str(
                        address
                        + "\\"
                        + os.listdir(address)[0])

            df = pd.read_csv(fileadd, delimiter=delimiter)
            time.sleep(10)
            empty_folder(address=address)
            return df


            break
        else:
            time.sleep(3)
            print("check again")
            pass

def read_and_remove_short_period(skiprows=0, sheet_name=0):
    time.sleep(5)
    for times in range(2):
        try:
            if len(os.listdir(f"{address}")) >= 1:
                if "/" in os.getcwd():
                    fileadd = str(
                        os.getcwd()
                        + "/inventory_files"
                        + "/"
                        + os.listdir(f"{address}")[0]
                    )
                else:
                    fileadd = str(
                        os.getcwd()
                        + "\inventory_files"
                        + "\\"
                        + os.listdir(f"{address}")[0]
                    )

                try:  # print(fileadd)
                    df = pd.read_csv(fileadd, skiprows=skiprows)
                    time.sleep(10)
                    empty_folder()
                    return df

                except:
                    df = pd.read_excel(
                        fileadd, sheet_name=sheet_name, engine="openpyxl"
                    )
                    time.sleep(10)
                    empty_folder()
                    return df
                # except:
                #     empty_folder()
                #     break

                break
            else:
                time.sleep(3)
                print("check again")
                pass
        except Exception as e:
            print("parser error or html error ")
            pass
            break


# this function is to scroll to element
def scroll_to_element(driver, element):
    script = "arguments[0].scrollIntoView(true);"
    driver.execute_script(script, element)


# this function makes sure that the element that you're trying to scroll to is
def scroll_into_view(driver, element_to_scroll_to):
    # Get the element's coordinates
    element_x = element_to_scroll_to.location["x"]
    element_y = element_to_scroll_to.location["y"]

    # Get the size of the viewport
    viewport_height = driver.execute_script("return window.innerHeight;")

    # Calculate the vertical scroll position to center the element
    scroll_y = element_y - (viewport_height / 2)

    # scroll_y = scroll_into_view(driver)

    # Scroll to the calculated position
    driver.execute_script("window.scrollTo(0, arguments[0]);", scroll_y)

"""
sheet_column = Specify the name of the column which you want to fetch the links from
sheet_tab_inside_each_spreadsheet_id = Specify the name of the sheet which you want the data for - the data will be apppended one below the other and is sent across!
# """
# def read_sheet_and_send_sheet_col_as_lst(spreadsheet_id , sheet_name , sheet_column , sheet_tab_inside_each_spreadsheet_id):

#     service_file_path = os.path.join(current_dir, "key.json")
#     gc = pygsheets.authorize(service_file=service_file_path)  
#     sh = gc.open_by_key(spreadsheet_id)
#     selected_sheet = sh.worksheet_by_title(sheet_name)
#     df = selected_sheet.get_as_df()
#     sheet_col_lst = df[f'{sheet_column}'].to_list()
#     #alternate
#     # sheet_col_lst_cleaned = [x for x in sheet_col_lst if x is not None and x!=''] 
#     sheet_col_lst_cleaned = list(filter(lambda x: x is not None and x != '', sheet_col_lst))
#     all_spreadsheet_id_lst = [x.split('/')[5] for x in sheet_col_lst_cleaned]

#     df_lst = []
#     index = 0
#     for x in all_spreadsheet_id_lst:
#         service_file_path = os.path.join(current_dir, f"key{index}.json")
#         gc = pygsheets.authorize(service_file=service_file_path)  
#         try:
#             sh = gc.open_by_key(x)
#             selected_sheet = sh.worksheet_by_title(sheet_tab_inside_each_spreadsheet_id)
#             df2 = selected_sheet.get_as_df()
#             if len(df2)>0:
#                 df_lst.append(df2)
#         except:
#             pass

#         index+=1
#         index=index%12

#     output = pd.concat(df_lst)
#     output.reset_index(inplace=True)
#     print(f"The length of df is :{len(df_lst)}")
#     return output


def read_sheet_and_send_sheet_col_as_lst(spreadsheet_id, sheet_name, sheet_column, sheet_tab_inside_each_spreadsheet_id,mode='concat'):
    def process_spreadsheet(index, x):
        index=index%12
        service_file_path = os.path.join(current_dir, f"key{index}.json")
        gc = pygsheets.authorize(service_file=service_file_path)
        try:
            sh = gc.open_by_key(x)
            selected_sheet = sh.worksheet_by_title(sheet_tab_inside_each_spreadsheet_id)
            df2 = selected_sheet.get_as_df()
            if len(df2) > 0:
                return df2
        except Exception as e:
            print(f"Error processing spreadsheet {x}: {e}")
            return None

    service_file_path = os.path.join(current_dir, "key.json")
    gc = pygsheets.authorize(service_file=service_file_path)
    sh = gc.open_by_key(spreadsheet_id)
    selected_sheet = sh.worksheet_by_title(sheet_name)
    df = selected_sheet.get_as_df()
    sheet_col_lst = df[f'{sheet_column}'].to_list()
    sheet_col_lst_cleaned = list(filter(lambda x: x is not None and x != '', sheet_col_lst))
    print(sheet_col_lst_cleaned)
    all_spreadsheet_id_lst = [x.split('/')[5] for x in sheet_col_lst_cleaned]

    df_lst = []
    with ThreadPoolExecutor(max_workers=12) as executor:
        futures = [executor.submit(process_spreadsheet, index, x) for index, x in enumerate(all_spreadsheet_id_lst)]
        for future in futures:
            result = future.result()
            if result is not None:
                df_lst.append(result)

    output = pd.concat(df_lst)

    # Reset index
    output.reset_index(inplace=True)
    print(f"The length of df is: {len(df_lst)}")
    if mode == 'concat':
        return output
    else:
        return df_lst

# result = read_sheet_and_send_sheet_col_as_lst(spreadsheet_id, sheet_name, sheet_column, sheet_tab_inside_each_spreadsheet_id)


def convert(i):
    i=str(i)
    if "K" in i:
        return int(float(i.replace("K", "")) * 1000)
    elif "M" in i:
        return int(float(i.replace("M", "")) * 1000000)
    elif "-" in i:
        return int(float(i.replace("-", "0")) * 1000000)
    elif "Lakhs" in i:
        return int(float(i.replace("Lakhs", "")) * 100000)
    elif "," in i:
        return int(float(i.replace(",", "")))
    elif "Crores" in i:
        return int(float(i.replace("Crores", "")) * 10000000)
    elif "Units" in i:
        return int(float(i.replace("Units", "")))
    else:
        return i
    
def take_screenshot(driver, name="error_screenshot"):
    screenshot_filename = f"{name}.png"
    driver.save_screenshot(screenshot_filename)
    return screenshot_filename

# def save_html_dump(driver, name="error_page_dump"):
#     html_dump_filename = f"{name}.pkl"
#     with open(html_dump_filename, "w") as f:
#         f.write(driver.page_source)
#     return html_dump_filename

def save_html_dump(driver, name="error_page_dump"):
    html_dump_filename = f"{name}.pkl"
    with open(html_dump_filename, "w", encoding="utf-8") as f:
        f.write(driver.page_source)
    return html_dump_filename
        
def error_mail_system(content,subject_messages):
    subject = subject_messages
    sender = 'bpautomationmail@gmail.com'
    receivers = ['deepak.s@blooprint.in', 'raj.p@blooprint.in' ,'abhishek.k@blooprint.in', 'vignesh.k@blooprint.in' ]
    yag = yagmail.SMTP(user=sender, password="uhbdvyyqdeefjkbz")
    yag.send(to=receivers, subject=subject,contents=content, attachments=['error_screenshot.png','error_page_dump.pkl'])
    
    
    
def error_screenshot_html_dump_and_send_mail(driver,subject='errors'):
    screenshot_file = take_screenshot(driver)
    html_dump_file = save_html_dump(driver)
    traceback_message = traceback.format_exc()
    time.sleep(1)
    error_mail_system(traceback_message,subject)
    os.remove(screenshot_file)
    os.remove(html_dump_file)

def read_sheet(spreadsheet_id,sheet_name,index=1):
    try:
        index=int(index)
        index=index%12
        service_file_path = os.path.join(os.getcwd(), f"key{index}.json")
        print(service_file_path)
        gc = pygsheets.authorize(service_file=service_file_path)
        sh = gc.open_by_key(spreadsheet_id)
        selected_sheet = sh.worksheet_by_title(sheet_name)
        df = selected_sheet.get_as_df()
        # database.rename_columns(df)
        return df
    except:
        pass


def converter(df, column):
    def convert_value(value):
        try:
            if value.strip() == '':
                return 0
            elif 'Lakh' in value:
                numeric_part = value.replace('Lakhs', '').replace('Lkhs', '').replace('Lakh', '')
                return float(numeric_part) * 100000
            elif 'Crore' in value:
                numeric_part = value.replace('Crores', '').replace('Cr', '').replace('Crore', '')
                return float(numeric_part) * 10000000
            else:
                return float(value)
        except ValueError:
            # Handle the case where conversion to float fails
            print(f"Unable to convert value: {value}")
            return None

    # Apply the conversion function to the specified column in the DataFrame
    df[column] = df[column].apply(convert_value)
    return df

def remove_commas(value):
    if isinstance(value, str):
        return value.replace(',', '')
    else:
        return value
    
def mail_send_without_attachment(content,subject_messages):
    subject = subject_messages
    sender = 'bpautomationmail@gmail.com'
    receivers = ['deepak.s@blooprint.in', 'raj.p@blooprint.in' ,'abhishek.k@blooprint.in', 'vignesh.k@blooprint.in']
    yag = yagmail.SMTP(user=sender, password="uhbdvyyqdeefjkbz")
    yag.send(to=receivers, subject=subject,contents=content)
        
    
    
    
#this function deletes all the respective sheets in a given column!
def delete_all_sheets_in_given_column(spreadsheet_id, sheet_name, sheet_column,sheets_to_keep):
    sh = gc.open_by_key(spreadsheet_id)
    selected_sheet = sh.worksheet_by_title(sheet_name)
    df = selected_sheet.get_as_df()
    sheet_col_lst = df[f'{sheet_column}'].to_list()
    # alternate
    # sheet_col_lst_cleaned = [x for x in sheet_col_lst if x is not None and x!='']
    sheet_col_lst_cleaned = list(
        filter(lambda x: x is not None and x != '', sheet_col_lst))

    # this will get all spreadsheet_id's for each column
    all_spreadsheet_id_lst = [x.split('/')[5] for x in sheet_col_lst_cleaned]

    for x in all_spreadsheet_id_lst:

        raw_sh = gc.open_by_key(x)

        #pass a list in sheets_to_keep
        sheet_to_keep = sheets_to_keep
        # Get all worksheets
        all_worksheets = raw_sh.worksheets()

        sheets_to_delete = []

        for worksheet in all_worksheets:
            if worksheet.title not in sheet_to_keep:
                sheets_to_delete.append(worksheet)

        # uncomment this later
        # for worksheet in all_worksheets:
        #     if worksheet.title not in sheet_to_keep:
        #         sheets_to_delete.append(worksheet)

        for worksheet in sheets_to_delete:
            raw_sh.del_worksheet(worksheet)

    print(f"All sheets except '{sheet_to_keep}' have been deleted.")


"""
This function sends a dataframe to ops team along with atachments that are fetched from the root directory if present, the attachments needs to passed as list
"""
ops_receivers = ['wasi.a@blooprint.in', 'yammenee.s@blooprint.in', 'ankita.s@blooprint.in',]
ads_receivers = ['sameer.a@blooprint.in' , 'paramesh.k@blooprint.in','ajay.m@blooprint.in' ,]
price_deviation_receivers = ['ankita.s@blooprint.in','yammenee.s@blooprint.in','deepak.s@blooprint.in','prakash.m@blooprint.in','ajay.m@blooprint.in','wasi.a@blooprint.in' , 'sameer.a@blooprint.in']
brand_level_inventory_and_sales_view_receivers = ['ankita.s@blooprint.in','sameer.a@blooprint.in','deepak.s@blooprint.in','prakash.m@blooprint.in','ajay.m@blooprint.in','wasi.a@blooprint.in']


def send_mail_to_team(df , receivers , subject , attachments = [], mode=1):
    # Email details
    # subject = 'Order Processing (Processed Order IDs of various brands)'
    sender = 'bpautomationmail@gmail.com'
    if mode == 1:
        if df is not None:
            # Check if `dfs` is a list of DataFrames or a single DataFrame
            if isinstance(df, list):
                dfs = df
                # Convert each DataFrame in the list to HTML and append
                html_content = ''.join([df.to_html(index=False).replace('\n', '') for df in dfs])
            else:
                # Convert single DataFrame to HTML
                html_content = df.to_html(index=False)

            # Replace newline characters
            html_content = html_content.replace('\n', '')

            # Connect to yagmail SMTP
            yag = yagmail.SMTP(user=sender, password="uhbdvyyqdeefjkbz")

            # Send email with DataFrame in the body
            yag.send(to=receivers, subject=subject, contents=df , attachments=attachments)
        else:
            yag = yagmail.SMTP(user=sender, password="uhbdvyyqdeefjkbz")

            # Send email with DataFrame in the body
            yag.send(to=receivers, subject=subject, attachments=attachments)
    else:
        yag = yagmail.SMTP(user=sender, password="uhbdvyyqdeefjkbz")

        # Send email with DataFrame in the body
        yag.send(to=receivers, subject=subject, contents=df , attachments=attachments)


def send_mail_to_team_with_headings(df, receivers, subject, attachments=[], mode=1, headings=None):
    """
    Sends an email with one or more DataFrames and optional headings.

    Parameters:
    - df: DataFrame or list of DataFrames to be sent.
    - receivers: List of email addresses to send the email to.
    - subject: Subject of the email.
    - attachments: List of files to attach (optional).
    - headings: List of headings for each DataFrame (optional).
    - mode: Determines whether to include DataFrame content (1 for HTML, other values for plain text).
    """
    sender = 'bpautomationmail@gmail.com'
    
    try:
        yag = yagmail.SMTP(user=sender, password="uhbdvyyqdeefjkbz")
        
        if mode == 1:
            if df is not None:
                if isinstance(df, list):
                    dfs = df
                    if headings is None or len(headings) != len(dfs):
                        headings = [None] * len(dfs)

                    html_content = ''
                    for i, dataframe in enumerate(dfs):
                        heading_html = f"<h3 style='margin:0;padding:0;'>{headings[i]}</h3>" if headings[i] else ''
                        df_html = dataframe.to_html(index=False, border=0).replace(
                            '<table border="0" class="dataframe">',
                            '<table style="width:100%;border-collapse:collapse;font-family:Arial,sans-serif;text-align:center;padding:0;margin:0;">'
                        ).replace(
                            '<th>', '<th style="border:1px solid #000;padding:0;margin:0;">'
                        ).replace(
                            '<td>', '<td style="border:1px solid #000;padding:0;margin:0;">'
                        ).replace(
                            '\n', ''
                        )
                        html_content += f"{heading_html}{df_html}"
                else:
                    df_html = df.to_html(index=False, border=0).replace(
                        '<table border="0" class="dataframe">',
                        '<table style="width:100%;border-collapse:collapse;font-family:Arial,sans-serif;text-align:center;padding:0;margin:0;">'
                    ).replace(
                        '<th>', '<th style="border:1px solid #000;padding:0;margin:0;">'
                    ).replace(
                        '<td>', '<td style="border:1px solid #000;padding:0;margin:0;">'
                    ).replace(
                        '\n', ''
                    )
                    html_content = df_html

                html_content = f"<html><body style='margin:0;padding:0;'>{html_content}</body></html>"

                yag.send(to=receivers, subject=subject, contents=[html_content], attachments=attachments)
            else:
                yag.send(to=receivers, subject=subject, attachments=attachments)
        else:
            yag.send(to=receivers, subject=subject, contents=df, attachments=attachments)
    
    except Exception as e:
        print(f"Error sending email: {e}")


def get_all_sheet(sheet_id):

  raw_sh=gc.open_by_key(sheet_id)

  # pull all sheet_names
  all_sheets=[]
  for worksheet in raw_sh.worksheets():
      all_sheets.append(worksheet.title)
  # pull all sheet data
  data_dict={}
  for sheet in all_sheets:
    data=raw_sh.worksheet_by_title(sheet).get_as_df()
    
    data_dict[sheet] = data
  return data_dict

def acos(df,spend,sales):
  df[spend] = pd.to_numeric(df[spend], errors='coerce').astype(float)
  df[sales] = pd.to_numeric(df[sales], errors='coerce').astype(float)
  df['ACOS']=(df[spend]/df[sales])*100
  df['ACOS']=df['ACOS'].replace([np.inf,-np.inf],np.nan).fillna(0)
  return df

def tacos(df,spend,gmv):
  df[spend] = pd.to_numeric(df[spend], errors='coerce').astype(float)
  df[gmv] = pd.to_numeric(df[gmv], errors='coerce').astype(float)
  df['TACOS']=(df[spend]/df[gmv])*100
  df['TACOS']=df['TACOS'].replace([np.inf,-np.inf],np.nan).fillna(0)
  return df

def cpc(df,spend,clicks):
  df[spend] = pd.to_numeric(df[spend], errors='coerce').astype(float)
  df[clicks] = pd.to_numeric(df[clicks], errors='coerce').astype(float)
  df['CPC']=(df[spend]/df[clicks])
  df['CPC']=df['CPC'].replace([np.inf,-np.inf],np.nan).fillna(0)
  return df


def cvr(df,units,clicks):
  df[units] = pd.to_numeric(df[units], errors='coerce').astype(float)
  df[clicks] = pd.to_numeric(df[clicks], errors='coerce').astype(float)
  df['CVR']=(df[units]/df[clicks])*100
  df['CVR']=df['CVR'].replace([np.inf,-np.inf],np.nan).fillna(0)
  return df

def ctr(df,impression,clicks):
  df[impression] = pd.to_numeric(df[impression], errors='coerce').astype(float)
  df[clicks] = pd.to_numeric(df[clicks], errors='coerce').astype(float)
  df['CTR']=(df[clicks]/df[impression])*100
  df['CTR']=df['CTR'].replace([np.inf,-np.inf],np.nan).fillna(0)
  return df

def asp(df,gmv,quantity):
  
  df['ASP']=(df[gmv].div(df[quantity]))
  df['ASP']=df['ASP'].replace([np.inf,-np.inf],np.nan).fillna(0)
  return df
def total_gmv_acv_per(df,gmv,daily_gmv_plan):
  df['Total GMV Ach%']=df[gmv]/df[daily_gmv_plan]*100
  df['Total GMV Ach%']=df['Total GMV Ach%'].replace([np.inf,-np.inf],np.nan).fillna(0)
  return df
def spend_acv_per(df,spend,daily_spend_plan):
  df['Spend Ach%']=df[spend]/df[daily_spend_plan]*100
  df['Spend Ach%']=df['Spend Ach%'].replace([np.inf,-np.inf],np.nan).fillna(0)
  return df



def is_next_available(driver,mode='normal'):
    # ensures if already next recursion is completed
    next_available=False
    next_is_finished= False
    # count_of_click_on_next=0
    try:
        # section=driver.find_elements(By.CSS_SELECTOR,"#content-container")
        all_elements=driver.find_elements(By.XPATH, "//*[contains(text(), 'Next')]")
        if all_elements[0].text == 'NextechDeals':
            print(len(all_elements))
            if len(all_elements)==2:
                next_available=False
                return next_available, next_is_finished
            if len(all_elements)>=3:
                next_available=True
                return next_available, next_is_finished
        driver.find_element(By.XPATH, "//*[contains(text(), 'Next')]")
        # count_of_click_on_next=1
        next_available=True
        return next_available, next_is_finished
    except:
        next_available=False
        
        try:
            texts=['Got It','Done','Last','Close']
            for text in texts:
                try:
                    try:
                    # if mode=='normal':
                        button=driver.find_element(By.XPATH, f"//button[contains(text(), '{text}')]")
                        # time.sleep(0.5)
                        button.click()
                        
                    except:
                        button=driver.find_element(By.XPATH, f"//*[contains(text(), '{text}')]").click()
                        
                    next_is_finished=True
                    break
                except:
                    pass
        except:
            pass    
        return next_available,next_is_finished

def recursive_next_click(driver,mode='normal'):
    next_available,next_is_finished=is_next_available(driver,mode)
    count=0
    while next_available and (count < 12):
        print(f"trying to click on next for {count} times")
        
        select_and_click_with_text(driver, 'Next')
        time.sleep(2)
        count+=1  # Add a 5-second delay after each click on 'Next'
        next_available,next_is_finished=is_next_available(driver,mode)
    return next_is_finished


def change_date_column_to_date_data_type(df):
    # Iterate through the columns and check for 'date' in column names
    for column in df.columns:
        if 'Date' in column.lower() or 'date' in column.lower():  # Check if 'date' is in the column name (case insensitive)
            try:
                df[column] = pd.to_datetime(df[column])
                df[column] = df[column].dt.strftime('%Y-%m-%d')
                print(column)
            except:
                pass
    return df



def get_dates_last_15_days():
    dates = []
    current_date = datetime.now()

    for i in range(15):
        date = current_date - timedelta(days=i)
        dates.append(date.strftime("%Y-%m-%d"))
    
    return dates


def get_start_end_dates_last_3_months():
    start_dates = []
    end_dates = []
    current_date = datetime.now()

    for i in range(3):
        # Calculate the starting date of the month
        start_date = current_date.replace(day=1)
        start_dates.append(start_date.strftime("%Y-%m-%d"))

        # Calculate the ending date of the month
        next_month = start_date.replace(day=28) + timedelta(days=4)  # Get the 28th day to avoid issues with month lengths
        end_date = next_month - timedelta(days=next_month.day)
        end_dates.append(end_date.strftime("%Y-%m-%d"))

        # Move to the previous month
        current_date = start_date - timedelta(days=1)

    return start_dates, end_dates




def click_on_n_days_ago(driver,n):

    report_period_button = driver.find_element(By.CSS_SELECTOR,'button#report-configuration-form\:report-period-control-component-0[data-takt-id="storm-ui-button"][data-takt-feature="unified-report-center:report-configuration-card:storm-ui-column-form"]')
    report_period_button.click()
    time.sleep(3)
    current_day = datetime.now()-timedelta(days=1)
    format_current_day = current_day.strftime("%Y-%m-%d")
    string_current_day = f'button[data-iso-date="{format_current_day}"]'
    before_60_days = current_day-timedelta(days=n)
    format_before_60_days = before_60_days.strftime("%Y-%m-%d")
    string_before_60_days = f'button[data-iso-date="{format_before_60_days}"]'

    left_arrow = driver.find_element(By.CSS_SELECTOR,'button[data-takt-id="storm-ui-date-picker-calendar-month-header-calendar-month-previous"]')
    left_arrow.click()

    before_60_days_button = driver.find_element(By.CSS_SELECTOR,string_before_60_days)
    before_60_days_button.click()

    right_arrow = driver.find_element(By.CSS_SELECTOR,'button[data-takt-id="storm-ui-date-picker-calendar-month-header-calendar-month-next"]')
    right_arrow.click()

    current_day_button = driver.find_element(By.CSS_SELECTOR,string_current_day)
    current_day_button.click()

    save_button = driver.find_element(By.CSS_SELECTOR,'button[data-takt-id="storm-ui-date-picker-confirmation-control-save"]')
    save_button.click()

def parallel_sheet_update(sheet_id,sheet_name,index):
    current_time = datetime.now()
    current_date_string = current_time.strftime('%Y-%m-%d')
    current_time_string = current_time.strftime('%H:%M:%S') 
    try:
        date_df=read_sheet(spreadsheet_id=sheet_id,sheet_name=sheet_name,index=index)
        try:
            last_updated_date = date_df['updated_date'][0]
        except:
            pass
        time.sleep(2)

        if( last_updated_date!=current_date_string):
            clear_worksheet(sheet_id=sheet_id,worksheet_names=sheet_name,index=index)
    except:
        pass



def clear_duplicate_for_all_account_sheet(index,portal):
    df = read_sheet('10GQo2STPzq8CCRfY0v8VHPHRD_k7BNODL4QJW35-aug','all account sheets',11)
    def get_all_sheet(sheet_id,index):
        # gc=general.gc
        # raw_sh=gc.open_by_key(sheet_id)
        index=index%12
        service_file_path = os.path.join(os.getcwd(), f"key{index}.json")
        print(service_file_path)
        gc = pygsheets.authorize(service_file=service_file_path)
        sh=gc.open_by_key(sheet_id)
        # pull all sheet_names
        all_sheets=[]
        for worksheet in sh.worksheets():
            all_sheets.append(worksheet.title)
        # pull all sheet data
        for sheet in all_sheets:
            data=sh.worksheet_by_title(sheet).get_as_df()
            data.drop_duplicates(inplace=True)           
            print(sheet)
            print(data)
            print_sheet(index,data,sheet,sheet_id,1,1,mode=1)
    
        # data_dict[sheet] = data
        # print_sheet(index,data,sheet,sheet_id,1,1,mode=1)
        # time.sleep(4)
        


    # Function to extract URLs from a string
    def extract_urls(text):
        if isinstance(text, str):
            return re.findall(r'/spreadsheets/d/([^/]+)', text)
        else:
            return []

    # Assuming df is your DataFrame
    df['fk_urls'] = df['fk brand sheet'].apply(extract_urls)
    df['az_urls'] = df['az brand sheet'].apply(extract_urls)

    # Flatten the lists of URLs
    fk_urls = [url for sublist in df['fk_urls'].tolist() for url in sublist]
    az_urls = [url for sublist in df['az_urls'].tolist() for url in sublist]
 
    lenfk=len(fk_urls)

    lenaz=len(az_urls)
    if(portal=='fk'):
        for url in fk_urls:
            if(lenfk>index):
                get_all_sheet(fk_urls[index],index)
                break
    else:
        for url in az_urls:
            if(lenaz>index):
                get_all_sheet(az_urls[index],index)
                break



def clear_duplicate_for_one_sheet(sheet_id,index):
    
    # def get_all_sheet(sheet_id,index):
    index=index%12
    # gc=general.gc
    # raw_sh=gc.open_by_key(sheet_id)
    service_file_path = os.path.join(os.getcwd(), f"key{index}.json")
    print(service_file_path)
    gc = pygsheets.authorize(service_file=service_file_path)
    sh=gc.open_by_key(sheet_id)
    # pull all sheet_names
    all_sheets=[]
    for worksheet in sh.worksheets():
        all_sheets.append(worksheet.title)
    # pull all sheet data
    for sheet in all_sheets:
        data=sh.worksheet_by_title(sheet).get_as_df()
        data.drop_duplicates(inplace=True)
        print(sheet)
        # print(data)
        print_sheet(index,data,sheet,sheet_id,1,1,mode=1)
        time.sleep(4)
    
        # data_dict[sheet] = data
        # for 
        # print_sheet(index,data,sheet,sheet_id,1,1,mode=1)
        # time.sleep(4)

    # get_all_sheet(sheet_id,index)
        




def get_connected_wifi_for_linux():
    
    # Execute system command to get Wi-Fi network information
    result = subprocess.run(["iwgetid", "-r"], capture_output=True, text=True)
    current_time = datetime.now()
    current_date_string = current_time.strftime('%Y-%m-%d')

    
    # Check if the command was successful
    if result.returncode == 0:
        wifi_name = result.stdout.strip()
        return wifi_name,current_date_string
        







def get_connected_wifi_for_windows():
    # Execute system command to get Wi-Fi network information
    result = subprocess.run(["netsh", "wlan", "show", "interfaces"], capture_output=True, text=True)
    current_time = datetime.now()
    current_date_string = current_time.strftime('%Y-%m-%d')
    
    # Check if the command was successful
    if result.returncode == 0:
        # Parsing the output to extract SSID
        lines = result.stdout.split('\n')
        for line in lines:
            if "SSID" in line:
                wifi_name = line.split(":")[1].strip()
                return wifi_name,current_date_string



def get_wifi_name():
    try:
        wifi_name,current_date_string=get_connected_wifi_for_linux()
    except:
        wifi_name,current_date_string=get_connected_wifi_for_windows()
    return wifi_name,current_date_string



    
def is_same_button_available(driver,search_text,mode='normal'):
    # ensures if already next recursion is completed
    next_available=False
    next_is_finished= False
    # count_of_click_on_next=0
    try:
        driver.find_element(By.XPATH, f"//*[contains(text(), '{search_text}')]")
        # count_of_click_on_next=1
        next_available=True
        return next_available, next_is_finished
    except:
        next_available=False
        
        try:
            texts=['Got It','Done','Last','Close']
            for text in texts:
                try:
                    try:
                      if mode=='normal':
                          button=driver.find_element(By.XPATH, f"//button[contains(text(), '{text}')]")
                          # time.sleep(0.5)
                          button.click()
                        
                    except:
                        button=driver.find_element(By.XPATH, f"//*[contains(text(), '{text}')]").click()
                        
                    next_is_finished=True
                    break
                except:
                    pass
        except:
            pass    
        return next_available,next_is_finished

def recursive_click(driver,search_text,mode='normal'):
    next_available,next_is_finished=is_same_button_available(driver,search_text,mode='normal')
    count=0
    while next_available or (count >=12):
        
        select_and_click_with_text(driver, search_text)
        time.sleep(1)
        count+=1  # Add a 5-second delay after each click on 'Next'
        next_available,next_is_finished=is_same_button_available(driver,search_text,mode='normal')
    return next_is_finished
def recursively_close(driver,search_text):
  recursive_click(driver,search_text,mode='normal')

def skip_on_seller_portal_base_function(driver):
  list_of_cross=['[aria-label="Skip"]','[title="Skip"]','[title="Close"] svg','.ReactModal__Content--after-open .eQZbrp > div','.HYMgb','#app .chlJSj span','#popover-content button',
                 '[data-id="CLOSE"]','[data-testid="closeIcon"]','.fa-close','.ReactModal__Overlay--after-open .bdJOfF button','.ReactModal__Overlay--after-open .ReactModal__Header__Close','.__floater__body button'
                   ]

  popup=False
  applied_css=None
  for css in list_of_cross:

    try:
      #click on any css
      try:
        
        driver.find_element(By.CSS_SELECTOR,css).click()
        time.sleep(1)
        applied_css=css
        popup=True
        try:
            iframe_switch_by_css(driver,'iframe')
            iframe_button=driver.find_element(By.CSS_SELECTOR,css)
            if iframe_button:
                iframe_button.click()
                time.sleep(1)
                popup=True

            iframe_switch_to_default_content(driver)

            print(f"closed pop up with {css}")
            time.sleep(1)
            popup=True
        except Exception as e:
            iframe_switch_to_default_content(driver)

        break
      except:
        pass  
    except:
      # print(f"3{css}")
      continue 

    
  return popup,applied_css
def skip_on_seller_portal(driver,reload=True):
  try:
      driver.find_element(By.XPATH, "//*[contains(text(), 'NextechDeals')]")
      print("Found NextechDeals on the page so using skip_on_seller_portal_nextechdeals")
      skip_on_seller_portal_nextechdeals(driver)
      return
  except:
      pass
  print("trying_to skip")
  
  try:
    next=driver.find_element(By.XPATH, "//*[contains(text(), 'Next')]")
    if next:
      recursive_next_click(driver)
  except:
    pass
  popup=True
  count =0
  previous_css='dummy1'
  applied_css='dummy'
  list_to_close=['Next','Continue']
  previous_and_new_css=False
  while (popup == True) and (count < 3):
    popup,applied_css=skip_on_seller_portal_base_function(driver)
    print(f"trying {count} times to close popup")
    
    print(previous_css,applied_css)
    if reload==True:
        if (previous_css!=applied_css):
            driver.refresh()
            time.sleep(4)
    if previous_css==applied_css:
      if previous_and_new_css == False:

        try:
            for each_text in list_to_close:
                recursively_close(driver,each_text)
        except:
            pass
        previous_and_new_css=True
    
    previous_css=applied_css
    # print(popup)
    print("pop up closed successfully")
    count +=1


def send_to_s3_bucket(bucket_name,file_name,df):
  aws_access_key_id = constants.aws_access_key_id
  aws_secret_access_key = constants.aws_secret_access_key
  bucket_name = bucket_name
  csv_buffer = StringIO()
  df.to_csv(csv_buffer, index=False)
  s3 = boto3.resource('s3', aws_access_key_id=aws_access_key_id, aws_secret_access_key=aws_secret_access_key)
  updated_file_name=file_name+"_"+datetime.now().strftime('%d_%m_%Y_%H_%M')+".csv"
  s3.Object(bucket_name, updated_file_name).put(Body=csv_buffer.getvalue())
    

def pull_table_from_DB(database_url,table_name):
    engine = create_engine(database_url)
    # Reflect the existing database schema
    metadata = MetaData()
    metadata.reflect(bind=engine)

    # Access the table you want to query
    table = Table(table_name, metadata, autoload=True, autoload_with=engine)

    # Build a select query
    select_query = table.select()

    # Execute the query and fetch the results
    with engine.connect() as connection:
        result = connection.execute(select_query)
        rows = result.fetchall()

    df=pd.DataFrame(rows)

    return df



def pull_latest_data_from_DB(database_url, table_name, account_name):
    engine = create_engine(database_url)
    
    # Construct SQL query to fetch rows with maximum updated_date for the specified account name
    sql_query = f"""
        SELECT * FROM {table_name} WHERE account_name = '{account_name}' AND updated_date = ( SELECT MAX(updated_date) FROM {table_name}WHERE account_name = '{account_name}') """
    
    # Execute the query and fetch the results
    with engine.connect() as connection:
        df = pd.read_sql(sql_query, connection)

    return df

    

def data_cleaning(df):

    for column in df.columns:
        if df[column].dtype == 'object':
            try:
                df[column]  = df[column].astype('str').str.replace('₹','').str.replace('Rs.','').str.replace(',','').astype("float").astype('int')
            except:
                pass 
    return df


def send_db_error_to_sheet(table_name, e, sheet_name='db_errors', sheet_id='1HQmJYjfF-fJQRu6CCqUW45CTIejcZS9ODY5B-mueRGM'):
    index = random.randint(1,12)
    error_dict = {"Table Name": f"{table_name}", "Error": f"{e}"}
    final_df = pd.DataFrame([error_dict])
    add_sheet_name(sheet_id, sheet_name, index)
    print_sheet(index, final_df, sheet_name, sheet_id, x=1, y=1, mode=3)


def send_empty_df_to_sheet(account_name, name):
    index = random.randint(0, 12)
    dict = {
            "account_name": account_name,
            "table_name": name
        }
    sheet_id = "1HQmJYjfF-fJQRu6CCqUW45CTIejcZS9ODY5B-mueRGM"
    sheet_name = "empty_dfs"
    df = pd.DataFrame([dict])
    add_sheet_name(sheet_id, sheet_name, index)
    print_sheet(index, df, sheet_name, sheet_id, x=1, y=1, mode=3)


def skip_on_seller_portal_nextechdeals(driver):
    css_selectors = [".cCBwan", ".HYMgb", ".gEbYQo"]
    ignore = ["Create Campaign", "Add Money"]
    for css_selector in css_selectors:
        for i in range(20):
            try:
                pop_ups = driver.find_elements(By.CSS_SELECTOR, css_selector)
                if pop_ups:
                    for pop_up in pop_ups:
                        if pop_up.text not in ignore:
                            try:
                                pop_up.click()
                                time.sleep(2)
                            except:
                                pass
                else:
                    break
            except:
                break


def get_month_date_ranges(no_of_months):
    today = datetime.today()
    current_year = today.year
    current_month = today.month

    result = []
    for month_offset in range(no_of_months-1, -1, -1):
        target_month = current_month - month_offset
        target_year = current_year

        while target_month <= 0:
            target_month += 12
            target_year -= 1

        # Start date is always the 1st
        start_date = datetime(target_year, target_month, 1).date()

        # If current month and year, end_date = yesterday
        if target_year == current_year and target_month == current_month:
            end_date = (today - timedelta(days=1)).date()
        else:
            last_day = calendar.monthrange(target_year, target_month)[1]
            end_date = datetime(target_year, target_month, last_day).date()

        result.append((start_date, end_date))
    
    return result
