#ifndef _DataDefinitionLanguage_H_
#define _DataDefinitionLanguage_H_

#include <algorithm>

class DataDefinitionLanguage {
public:
	bool Create();
	bool Insert();
	bool Drop();
	void SetOutput();

private:
	bool exists(const char* relName);

	static inline std::string &ltrim(std::string &s) {
		s.erase(s.begin(), std::find_if(s.begin(), s.end(), std::not1(std::ptr_fun<int, int>(std::isspace))));
		return s;
	}

	static inline std::string &rtrim(std::string &s) {
		s.erase(std::find_if(s.rbegin(), s.rend(), std::not1(std::ptr_fun<int, int>(std::isspace))).base(), s.end());
		return s;
	}

	static inline std::string &trim(std::string &s) {
		return ltrim(rtrim(s));
	}
};


#endif