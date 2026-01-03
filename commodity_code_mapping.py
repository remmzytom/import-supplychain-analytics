"""
Commodity Code to Industry Sector Mapping
Maps commodity codes to SITC-based industries for import data analysis
Uses HS (Harmonized System) codes and maps to SITC (Standard International Trade Classification)

Hybrid Approach:
- Uses SITC Sections (0-9) as primary industry groups
- Breaks down Section 6 (Manufactured Goods) by SITC Divisions (2-digit) for better insights
"""

# SITC Section mapping (HS Chapter to SITC Section)
# Maps HS chapters to SITC sections based on first digit
HS_CHAPTER_TO_SITC_SECTION = {
    # Section 0: Food and Live Animals
    '01': '0',  # Live animals
    '02': '0',  # Meat and meat preparations
    '03': '0',  # Fish, crustaceans, molluscs
    '04': '0',  # Dairy products; birds' eggs
    '05': '0',  # Products of animal origin
    '06': '0',  # Live trees and other plants
    '07': '0',  # Edible vegetables
    '08': '0',  # Edible fruit and nuts
    '09': '0',  # Coffee, tea, mate and spices
    '10': '0',  # Cereals
    '11': '0',  # Products of the milling industry
    '12': '0',  # Oil seeds and oleaginous fruits
    '13': '0',  # Lac; gums, resins
    '14': '0',  # Vegetable plaiting materials
    '15': '4',  # Animal or vegetable fats and oils
    '16': '0',  # Preparations of meat, fish
    '17': '0',  # Sugars and sugar confectionery
    '18': '0',  # Cocoa and cocoa preparations
    '19': '0',  # Preparations of cereals, flour
    '20': '0',  # Preparations of vegetables, fruit
    '21': '0',  # Miscellaneous edible preparations
    '22': '1',  # Beverages, spirits and vinegar
    '23': '0',  # Residues and waste from food industries
    '24': '1',  # Tobacco and manufactured tobacco substitutes
    
    # Section 1: Beverages and Tobacco (covered above: 22, 24)
    
    # Section 2: Crude Materials (Except Fuels)
    '25': '2',  # Salt; sulfur; earths and stone
    '26': '2',  # Ores, slag and ash
    '27': '3',  # Mineral fuels, mineral oils (Section 3)
    '28': '5',  # Inorganic chemicals (Section 5)
    '29': '5',  # Organic chemicals (Section 5)
    '30': '5',  # Pharmaceutical products (Section 5)
    '31': '5',  # Fertilizers (Section 5)
    '32': '5',  # Tanning or dyeing extracts (Section 5)
    '33': '5',  # Essential oils and resinoids (Section 5)
    '34': '5',  # Soap, organic surface-active agents (Section 5)
    '35': '5',  # Albuminoidal substances (Section 5)
    '36': '5',  # Explosives (Section 5)
    '37': '5',  # Photographic or cinematographic goods (Section 5)
    '38': '5',  # Miscellaneous chemical products (Section 5)
    '39': '5',  # Plastics and articles thereof (Section 5)
    '40': '6',  # Rubber and articles thereof (Section 6)
    '41': '6',  # Raw hides and skins (Section 6)
    '42': '6',  # Articles of leather (Section 6)
    '43': '6',  # Furskins and artificial fur (Section 6)
    '44': '2',  # Wood and articles of wood (Section 2)
    '45': '2',  # Cork and articles of cork (Section 2)
    '46': '2',  # Manufactures of straw (Section 2)
    '47': '2',  # Pulp of wood (Section 2)
    '48': '6',  # Paper and paperboard (Section 6)
    '49': '6',  # Printed books, newspapers (Section 6)
    
    # Section 3: Mineral Fuels (covered above: 27)
    
    # Section 4: Animal and Vegetable Oils (covered above: 15)
    
    # Section 5: Chemicals (covered above: 28-39)
    
    # Section 6: Manufactured Goods Classified by Material
    '50': '6',  # Silk (Section 6)
    '51': '6',  # Wool, fine or coarse animal hair (Section 6)
    '52': '6',  # Cotton (Section 6)
    '53': '6',  # Other vegetable textile fibers (Section 6)
    '54': '6',  # Man-made filaments (Section 6)
    '55': '6',  # Man-made staple fibers (Section 6)
    '56': '6',  # Wadding, felt and nonwovens (Section 6)
    '57': '6',  # Carpets and other textile floor coverings (Section 6)
    '58': '6',  # Special woven fabrics (Section 6)
    '59': '6',  # Impregnated, coated fabrics (Section 6)
    '60': '6',  # Knitted or crocheted fabrics (Section 6)
    '61': '8',  # Articles of apparel, knitted (Section 8)
    '62': '8',  # Articles of apparel, not knitted (Section 8)
    '63': '6',  # Other made up textile articles (Section 6)
    '64': '8',  # Footwear, gaiters (Section 8)
    '65': '8',  # Headgear and parts thereof (Section 8)
    '66': '8',  # Umbrellas, walking sticks (Section 8)
    '67': '8',  # Prepared feathers and down (Section 8)
    '68': '6',  # Articles of stone, plaster, cement (Section 6)
    '69': '6',  # Ceramic products (Section 6)
    '70': '6',  # Glass and glassware (Section 6)
    '71': '9',  # Natural or cultured pearls (Section 9)
    '72': '6',  # Iron and steel (Section 6)
    '73': '6',  # Articles of iron or steel (Section 6)
    '74': '6',  # Copper and articles thereof (Section 6)
    '75': '6',  # Nickel and articles thereof (Section 6)
    '76': '6',  # Aluminum and articles thereof (Section 6)
    '77': '6',  # Reserved for future use
    '78': '6',  # Lead and articles thereof (Section 6)
    '79': '6',  # Zinc and articles thereof (Section 6)
    '80': '6',  # Tin and articles thereof (Section 6)
    '81': '6',  # Other base metals (Section 6)
    '82': '6',  # Tools, implements, cutlery (Section 6)
    '83': '6',  # Miscellaneous articles of base metal (Section 6)
    
    # Section 7: Machinery and Transport Equipment
    '84': '7',  # Nuclear reactors, boilers, machinery (Section 7)
    '85': '7',  # Electrical machinery and equipment (Section 7)
    '86': '7',  # Railway or tramway locomotives (Section 7)
    '87': '7',  # Vehicles other than railway (Section 7)
    '88': '7',  # Aircraft, spacecraft (Section 7)
    '89': '7',  # Ships, boats and floating structures (Section 7)
    
    # Section 8: Miscellaneous Manufactured Articles
    '90': '8',  # Optical, photographic, measuring instruments (Section 8)
    '91': '8',  # Clocks and watches (Section 8)
    '92': '8',  # Musical instruments (Section 8)
    '93': '8',  # Arms and ammunition (Section 8)
    '94': '8',  # Furniture; bedding, mattresses (Section 8)
    '95': '8',  # Toys, games and sports requisites (Section 8)
    '96': '8',  # Miscellaneous manufactured articles (Section 8)
    '97': '9',  # Works of art, collectors' pieces (Section 9)
    '98': '9',  # Special classification provisions (Section 9)
    '99': '9',  # Special classification provisions (Section 9)
}

# SITC Section names
SITC_SECTION_NAMES = {
    '0': 'Food and Live Animals',
    '1': 'Beverages and Tobacco',
    '2': 'Crude Materials (Except Fuels)',
    '3': 'Mineral Fuels and Lubricants',
    '4': 'Animal and Vegetable Oils, Fats and Waxes',
    '5': 'Chemicals and Related Products',
    '6': 'Manufactured Goods Classified by Material',
    '7': 'Machinery and Transport Equipment',
    '8': 'Miscellaneous Manufactured Articles',
    '9': 'Commodities and Transactions Not Classified Elsewhere',
}


def map_commodity_code_to_sitc_section(commodity_code):
    """
    Map commodity code (HS code) to SITC Section (first digit)
    
    Args:
        commodity_code: Commodity code (string or numeric)
        
    Returns:
        str: SITC Section code (0-9)
    """
    import pandas as pd
    
    if pd.isna(commodity_code) or commodity_code == '':
        return '9'  # Unclassified
    
    code_str = str(commodity_code).strip()
    
    # Get 2-digit chapter code
    if len(code_str) >= 2:
        chapter = code_str[:2]
        return HS_CHAPTER_TO_SITC_SECTION.get(chapter, '9')
    
    # If only 1 digit, try to map directly
    if len(code_str) == 1:
        return code_str if code_str in SITC_SECTION_NAMES else '9'
    
    return '9'  # Unclassified


def get_sitc_section_name(sitc_section_code):
    """
    Get SITC Section name from section code
    
    Args:
        sitc_section_code: SITC Section code (0-9)
        
    Returns:
        str: SITC Section name
    """
    return SITC_SECTION_NAMES.get(str(sitc_section_code), 'Unclassified')


def get_all_sitc_sections():
    """
    Get all SITC sections with codes and names
    
    Returns:
        dict: SITC section codes to names mapping
    """
    return SITC_SECTION_NAMES.copy()


# SITC Division mapping for Section 6 breakdown (HS Chapter to SITC Division)
# Maps HS chapters to SITC 2-digit division codes for Section 6 only
HS_CHAPTER_TO_SITC_DIVISION = {
    # Section 6: Manufactured Goods Classified by Material
    # Division 61: Leather
    '41': '61',  # Raw hides and skins
    '42': '61',  # Articles of leather
    '43': '61',  # Furskins
    
    # Division 62: Rubber
    '40': '62',  # Rubber and articles thereof
    
    # Division 63: Cork and Wood
    '44': '63',  # Wood and articles of wood
    '45': '63',  # Cork and articles of cork
    
    # Division 64: Paper
    '47': '64',  # Pulp of wood
    '48': '64',  # Paper and paperboard
    '49': '64',  # Printed books, newspapers
    
    # Division 65: Textiles
    '50': '65',  # Silk
    '51': '65',  # Wool
    '52': '65',  # Cotton
    '53': '65',  # Other vegetable textile fibers
    '54': '65',  # Man-made filaments
    '55': '65',  # Man-made staple fibers
    '56': '65',  # Wadding, felt and nonwovens
    '57': '65',  # Carpets
    '58': '65',  # Special woven fabrics
    '59': '65',  # Impregnated, coated fabrics
    '60': '65',  # Knitted or crocheted fabrics
    '63': '65',  # Other made up textile articles
    
    # Division 66: Non-metallic Mineral Manufactures
    '68': '66',  # Articles of stone, plaster, cement
    '69': '66',  # Ceramic products
    '70': '66',  # Glass and glassware
    
    # Division 67: Iron and Steel
    '72': '67',  # Iron and steel
    '73': '67',  # Articles of iron or steel
    
    # Division 68: Non-ferrous Metals
    '74': '68',  # Copper
    '75': '68',  # Nickel
    '76': '68',  # Aluminum
    '78': '68',  # Lead
    '79': '68',  # Zinc
    '80': '68',  # Tin
    '81': '68',  # Other base metals
    
    # Division 69: Manufactures of Metal
    '82': '69',  # Tools, implements, cutlery
    '83': '69',  # Miscellaneous articles of base metal
}

# Friendly names for SITC Divisions (Section 6 breakdown)
SITC_DIVISION_NAMES = {
    '61': 'Leather & Related Products',
    '62': 'Rubber Products',
    '63': 'Wood & Cork Products',
    '64': 'Paper & Paper Products',
    '65': 'Textiles & Textile Products',
    '66': 'Non-Metallic Mineral Products',
    '67': 'Iron & Steel',
    '68': 'Non-Ferrous Metals',
    '69': 'Metal Manufactures',
}

# Friendly industry names for SITC Sections (non-Section 6)
SITC_INDUSTRY_NAMES = {
    '0': 'Food & Live Animals',
    '1': 'Beverages & Tobacco',
    '2': 'Crude Materials (Except Fuels)',
    '3': 'Mineral Fuels & Lubricants',
    '4': 'Animal & Vegetable Oils',
    '5': 'Chemicals & Related Products',
    '6': 'Manufactured Goods',  # Will be replaced by division names
    '7': 'Machinery & Transport Equipment',
    '8': 'Miscellaneous Manufactured Articles',
    '9': 'Commodities Not Classified Elsewhere',
}


def map_commodity_code_to_sitc_industry(commodity_code):
    """
    Map commodity code to SITC-based industry with friendly names
    Uses SITC Sections (0-9) as primary grouping
    Breaks down Section 6 using SITC Divisions (2-digit) for better insights
    
    Args:
        commodity_code: Commodity code (string or numeric)
        
    Returns:
        str: Industry name (friendly, readable)
    """
    import pandas as pd
    
    if pd.isna(commodity_code) or commodity_code == '':
        return 'Commodities Not Classified Elsewhere'
    
    # Get SITC section first
    sitc_section = map_commodity_code_to_sitc_section(commodity_code)
    
    # If Section 6, break down by division
    if sitc_section == '6':
        # Get HS chapter code - extract from original code, not padded
        code_str = str(commodity_code).strip()
        code_int = None
        
        try:
            code_int = int(code_str)
        except ValueError:
            pass
        
        # Extract chapter code based on code length
        # For 5-digit codes: first 2 digits are chapter (e.g., 52010 → 52)
        # For 4-digit codes: first 2 digits are chapter (e.g., 5201 → 52, not 05)
        # For 3-digit codes: might need padding, but extract chapter from original
        if code_int is not None:
            code_len = len(code_str)
            if code_len >= 5:
                # 5+ digits: first 2 digits are chapter
                chapter = code_str[:2]
            elif code_len == 4:
                # 4 digits: first 2 digits are chapter (e.g., 5201 → 52)
                chapter = code_str[:2]
            elif code_len == 3:
                # 3 digits: extract first digit as potential chapter
                chapter = code_str[0].zfill(2) if len(code_str) >= 1 else '00'
            elif code_len == 2:
                # 2 digits: this IS the chapter
                chapter = code_str
            else:
                # 1 digit: pad to 2
                chapter = code_str.zfill(2)
        else:
            # Not numeric, extract first 2 characters
            chapter = code_str[:2] if len(code_str) >= 2 else code_str.zfill(2)
        
        # Map chapter to SITC division
        sitc_division = HS_CHAPTER_TO_SITC_DIVISION.get(chapter)
        if sitc_division:
            return SITC_DIVISION_NAMES.get(sitc_division, 'Manufactured Goods')
        
        # Fallback: if chapter not found, return generic
        return 'Manufactured Goods'
    
    # For non-Section 6, return friendly section name
    return SITC_INDUSTRY_NAMES.get(sitc_section, 'Commodities Not Classified Elsewhere')


def get_all_sitc_industries():
    """
    Get list of all SITC-based industries (sections + Section 6 divisions)
    
    Returns:
        list: List of all industry names
    """
    industries = []
    # Add non-Section 6 industries
    for section in ['0', '1', '2', '3', '4', '5', '7', '8', '9']:
        industries.append(SITC_INDUSTRY_NAMES[section])
    # Add Section 6 divisions
    industries.extend(SITC_DIVISION_NAMES.values())
    return sorted(industries)


if __name__ == "__main__":
    # Test the SITC-based mapping functions
    test_codes = [2710, 8703, 8517, 3004, 7201, 101, 5201, 2801, 8425, 8469, 4801, 6801, 7401]
    
    print("="*60)
    print("Commodity Code to SITC Section Mapping Test:")
    print("="*60)
    for code in test_codes:
        sitc_section = map_commodity_code_to_sitc_section(code)
        section_name = get_sitc_section_name(sitc_section)
        print(f"  {code} → SITC Section {sitc_section}: {section_name}")
    
    print("\n" + "="*60)
    print("Commodity Code to SITC-Based Industry (Hybrid) Test:")
    print("(Uses SITC Sections, breaks down Section 6 by divisions)")
    print("="*60)
    for code in test_codes:
        industry = map_commodity_code_to_sitc_industry(code)
        print(f"  {code} → {industry}")
