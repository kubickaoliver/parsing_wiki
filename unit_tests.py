import unittest
from pyspark_parser import parse_parameter_value, parsing_wiki, search_simular_vehicle


REGEX_TEST_INPUT = """Row(revision=Row(text="{{Unreferenced|date=December 2009}}\\n{{Infobox automobile\\n|name         = Volvo Philip\\n|image        = Goteborg Volvo Museum 59 Philip.jpg\\n|caption      = Volvo Philip at the [[Volvo Museum]]. <small>(April 2014)</small>\\n|manufacturer = [[Volvo Cars]] \\n|production   = 1952\\n|designer     = [[Jan Wilsgaard]]\\n|class        =\\n|body_style   = 4 door sedan\\n|layout       = [[FR layout]]\\n|platform     =\\n|related      =\\n|engine       = 3.6L ''[[Volvo B36 engine|B8B]]'' [[V8 engine|V8]]\\n|transmission =\\n|wheelbase    =\\n|length       =\\n|width        =\\n|height       =\\n|weight       =\\n}}\\n\nThe '''Volvo Philip''' was a [[concept car]] that was built by [[Volvo Cars|Volvo]] in 1952. It was designed especially for the [[United States]] market and so was fitted with a prototype [[V8 engine|V8]] engine called the [[Volvo B36 engine|B8B]], which produced {{Convert|120|hp|abbr=on}} at 4000 rpm and was fitted with [[whitewall tires]] and a hint of [[Car tailfin|tailfin]]s. \\n\nThe design was inspired by American cars and was similar to the 1951 [[Kaiser Motors|Kaiser]]. The designer was [[Jan Wilsgaard]], who also designed the [[Volvo Amazon]]. However, it was cancelled by the board and never reached production, with only one car being made. That car was used for several years by the board at [[Bolinder-Munktell]] in [[Eskilstuna]] and is now preserved at the [[Volvo Museum]] in [[Gothenburg]]. \\n\nThis vehicle was hand built under extreme secrecy and was subjected to thorough testing. However, the V8 engine entered production in 1956 and was used for a truck, the [[Volvo Snabbe]], as well as for boats. It was known for being strong and reliable but also for high [[fuel consumption]]. Production of the engine had ceased by 1973.\\n\\n[[Category:Volvo concept vehicles|Philip]]\\n\\n{{Volvo cars}}{{Classicpow-auto-stub}}"))"""

class ParserTests(unittest.TestCase):
    # Testing if all parameters from specific infobox are correct
    def test_parse_infobox(self):
        result = parsing_wiki(REGEX_TEST_INPUT)
        self.assertEqual(
            result,
            {'name': 'volvo philip', 'manufacturer': ['volvo cars'], 'class': [], 'layout': ['fr layout'], 'production_year': 1952, 'related': []},
            msg='Test: Regex infobox parsing faild'
        )

    # Test if name parameter's entities are correctly parsed
    def test_parameter_regex_parsing(self):
        result = parse_parameter_value('name', REGEX_TEST_INPUT)
        self.assertEqual(
            result,
            ['Volvo Philip'],
            msg='Test: Regex parameters parsing failed'
        )

    # Test searching within index (big dataset)
    def test_finding_in_index_big_dataset(self):
        test_correct_result = [{'name': 'porsche taycan', 'manufacturer': ['porsche'], 'class': ['executive car', 'e-segment'], 'layout': ['rear-motor', 'rear-wheel drive', 'taycan', 'dual-motor', 'all-wheel drive', 'taycan 4', 'gts', 'turbo & turbo s'], 'production_year': 2019, 'related': ['audi e-tron gt']}, {'name': 'audi 100', 'manufacturer': ['auto union', 'gesellschaft mit beschränkter haftung', 'gmbh', 'audi', 'nsu motorenwerke', 'nsu', 'auto union', 'aktiengesellschaft', '1969–1985', 'audi'], 'class': ['luxury vehicle', 'mid-size luxury', '2fexecutive cars', 'mid-size luxury', 'executive car', 'e-segment'], 'layout': ['tlongitudinal front engine', 'front-wheel drive', 'quattro', 'four-wheel-drive system', 'quattro', 'four-wheel-drive'], 'production_year': 1968, 'related': []}, {'name': 'audi a7', 'manufacturer': ['audi'], 'class': ['executive car', 'e-segment', 'coupe'], 'layout': ['ubl'], 'production_year': 2010, 'related': ['ubl']}]
        result = search_simular_vehicle('./parsed_vehicles_big_dataset')
        self.assertEqual(
            result,
            test_correct_result,
            msg='Test: Index failed within big dataset'
        )
        

if __name__ == '__main__':
    unittest.main()