"""Data cleaning functions for staging ETL"""
import re
import pandas as pd

# Các từ công nghệ cần giữ nguyên viết hoa
TECH_WORDS = {
    'PHP', 'JAVA', 'PYTHON', 'AWS', 'SQL', 'HTML', 'CSS', 'JS', 'UI', 'UX', 
    'AI', 'ML', 'IOS', 'API', 'IT', 'CNTT', 'REACT', 'VUE', 'ANGULAR', 'NODE',
    'DEVOPS', 'QA', 'BA', 'PM', 'HR', 'ERP', 'CRM', 'SAP', 'BTP', 'CAP',
    'NET', 'GO', 'RUST', 'KOTLIN', 'SWIFT', 'FLUTTER', 'DART',
    'NODEJS', 'NEXTJS', 'REACTJS', 'VUEJS', 'TYPESCRIPT', 'JAVASCRIPT',
    'MONGODB', 'MYSQL', 'POSTGRESQL', 'REDIS', 'DOCKER', 'K8S', 'KUBERNETES',
    'GIT', 'CI', 'CD', 'AWS', 'GCP', 'AZURE', 'IOT', 'MEP', 'HVAC', 'BIM',
    'ODOO', 'LARAVEL', 'SPRING', 'SPRINGBOOT', 'DJANGO', 'FLASK', 'FASTAPI',
    'MSB', 'SI', 'LG', 'CNS', 'MISA', 'FPT', 'VIETTEL', 'VNPT', 'IS', 'IEC',
    'ABI', 'DNSE', 'BRSE', 'SQA'
}

# Các từ viết tắt công ty cần giữ nguyên
COMPANY_ABBR = {
    'TNHH', 'CP', 'CPĐT', 'JSC', 'LLC', 'INC', 'LTD', 'CO', 'CORP',
    'BPO', 'IT', 'AI', 'IOT', 'ERP', 'CRM'
}


def clean_title(title: str) -> str:
    """ Làm sạch tiêu đề công việc """
    if pd.isna(title) or not title:
        return ""
    
    title = str(title).strip()
    
    # 1. Loại bỏ phần salary/location
    remove_patterns = [
        r'\s*[-–]\s*Thu Nhập.*$',
        r'\s*[-–]\s*Upto.*$', 
        r'\s*[-–]\s*Up to.*$',
        r'\s*[-–]\s*Salary.*$',
        r'\s*[-–]\s*Lương.*$',
        r'\s*[-–]\s*Tại\s+(Hà Nội|HCM|Hồ Chí Minh|Đà Nẵng).*$',
        r'\s*[-–]\s*Từ\s+\d+.*$',
        r'\s*[-–]\s*Tối Thiểu.*$',
        r'\s*\[Hà Nội\].*$',
        r'\s*\[HCM\].*$',
        r'\s*\[Hồ Chí Minh\].*$',
        r'\s*\|\s*Thu Nhập.*$',
        r'\s*\|\s*Lương.*$',
        r'\s+Thu Nhập Từ\s+\d+.*$',
        r'\s+Lương Upto.*$',
        r'\s+Onboard Sau Tết.*$',
    ]
    
    for pattern in remove_patterns:
        title = re.sub(pattern, '', title, flags=re.IGNORECASE)
    
    # 2. Thêm space trước ngoặc nếu thiếu
    title = re.sub(r'(\w)\(', r'\1 (', title)
    
    # 3. Thêm space sau dấu : và ,
    title = re.sub(r':(\w)', r': \1', title)
    title = re.sub(r',(\w)', r', \1', title)
    
    # 4. Bảo vệ C++ và C#
    title = re.sub(r'C\+\+', '___CPLUSPLUS___', title)
    title = re.sub(r'C#', '___CSHARP___', title)
    
    # 5. Loại bỏ ký tự đặc biệt
    title = re.sub(r'[^\w\s\(\)\/\-\.\$,:]', ' ', title)
    
    # 6. Khôi phục C++ và C#
    title = title.replace('___CPLUSPLUS___', 'C++')
    title = title.replace('___CSHARP___', 'C#')
    
    # 7. Chuẩn hóa .NET
    title = re.sub(r'\.NET\b', '.NET', title, flags=re.IGNORECASE)
    
    # 8. Loại bỏ khoảng trắng thừa
    title = re.sub(r'\s+', ' ', title).strip()
    
    return title


def clean_company_name(name: str) -> str:
    """ Chuẩn hóa tên công ty """
    if pd.isna(name) or not name:
        return ""
    
    name = str(name).strip()
    
    # 1. Loại bỏ ký tự đặc biệt
    name = re.sub(r'[^\w\s\(\)\[\]\-\/\.,&+#]', ' ', name)
    name = re.sub(r'\s+', ' ', name).strip()
    
    # 2. Xóa từ khóa tuyển dụng
    for pattern in [r'\btuyển\s+dụng\b', r'\bcần\s+tuyển\b', r'\bđang\s+tuyển\b', r'\bhot\b', r'\bgấp\b']:
        name = re.sub(pattern, '', name, flags=re.IGNORECASE)
    
    # 3. Chuẩn hóa viết hoa/thường
    words = name.split()
    result = []
    
    for word in words:
        word_upper = word.upper()
        if word_upper in COMPANY_ABBR or word_upper in TECH_WORDS:
            result.append(word_upper)
        elif re.match(r'^[A-Z][a-z]+[A-Z]', word):
            result.append(word)
        elif re.match(r'^\d+\w*$', word):
            result.append(word.upper())
        elif len(word) > 1:
            result.append(word.capitalize())
        else:
            result.append(word.upper())
    
    name = ' '.join(result).strip()
    
    # 4. Fix pattern đặc biệt
    name = re.sub(r'\bCông TY\b', 'Công Ty', name)
    name = re.sub(r'\bNgân HÀNG\b', 'Ngân Hàng', name)
    name = re.sub(r'\s+', ' ', name).strip()
    
    return name
