/*!
*/

#pragma once

#include <algorithm> 
#include <core/compressed_column.hpp>
#include <boost/serialization/utility.hpp>

namespace CoGaDB{
	

/*!
 *  \brief     This class represents a bit vector compressed column with type T.
 */	
template<class T>
class BitVectorCompressedColumn : public CompressedColumn<T>{
	public:
	/***************** constructors and destructor *****************/
	BitVectorCompressedColumn(const std::string& name, AttributeType db_type);
	virtual ~BitVectorCompressedColumn();

	virtual bool insert(const boost::any& new_Value);
	virtual bool insert(const T& new_value);
	template <typename InputIterator>
	bool insert(InputIterator first, InputIterator last);

	virtual bool update(TID tid, const boost::any& new_value);
	virtual bool update(PositionListPtr tid, const boost::any& new_value);	
	
	virtual bool remove(TID tid);
	//assumes tid list is sorted ascending
	virtual bool remove(PositionListPtr tid);
	virtual bool clearContent();

	virtual const boost::any get(TID tid);
	//virtual const boost::any* const getRawData()=0;
	virtual void print() const throw();
	virtual size_t size() const throw();
	virtual unsigned int getSizeinBytes() const throw();

	virtual const ColumnPtr copy() const;

	virtual bool store(const std::string& path);
	virtual bool load(const std::string& path);


	virtual T& operator[](const int index);

	private:

	std::vector< std::pair<T, std::vector<bool>> > val_vector;

};


/***************** Start of Implementation Section ******************/

	
	template<class T>
	BitVectorCompressedColumn<T>::BitVectorCompressedColumn(const std::string& name, AttributeType db_type) : CompressedColumn<T>(name, db_type){

	}

	template<class T>
	BitVectorCompressedColumn<T>::~BitVectorCompressedColumn(){

	}

	template<class T>
	bool BitVectorCompressedColumn<T>::insert(const boost::any&){

		return false;
	}

	template<class T>
	bool BitVectorCompressedColumn<T>::insert(const T& new_value){
		std::vector<bool> bit_vector;
		if(val_vector.empty()) 
		{
			bit_vector.push_back(true);
			val_vector.push_back(std::make_pair(new_value, bit_vector));
		}
		else
		{
			bool flag = false;
			for(unsigned int i = 0; i < val_vector.size(); i++)
			{
				if (val_vector[i].first != new_value)
				{
					val_vector[i].second.push_back(false);

				}
				else 
				{
					val_vector[i].second.push_back(true);
					flag = true;
				}
			}
			if (flag == false)
			{
				/*for(unsigned int i = 0; i < val_vector.size(); i++)
				{
					bit_vector.push_back(false);
				}*/
				
				bit_vector.resize(val_vector[0].second.size()-1, false);
				bit_vector.push_back(true);
				val_vector.push_back(std::make_pair(new_value, bit_vector));
			}
		}
		return false;
	}

	template <typename T> 
	template <typename InputIterator>
	bool BitVectorCompressedColumn<T>::insert(InputIterator , InputIterator ){
		
		return true;
	}

	template<class T>
	const boost::any BitVectorCompressedColumn<T>::get(TID){

		return boost::any();
	}

	template<class T>
	void BitVectorCompressedColumn<T>::print() const throw(){
		std::cout << "| "<<"compressed " << this->name_ << " |" << std::endl;
		//std::cout << "________________________" << std::endl;
		for(unsigned int i = 0;i < val_vector.size(); i++)
		{
			std::cout << "| " << val_vector[i].first << " | ";
			for (unsigned int j = 0; j < val_vector[i].second.size(); j++)
			{
				std::cout << val_vector[i].second[j];
			}
			std::cout << std::endl;
		}
		std::cout << "size of compressed column in Bytes: " << getSizeinBytes() << std::endl;
		std::cout << std::endl;
	}
	template<class T>
	size_t BitVectorCompressedColumn<T>::size() const throw(){
		size_t n = 0;
		if (val_vector.size() != 0)
		{
			n = val_vector[0].second.size();
		}
		return n;
	}
	template<class T>
	const ColumnPtr BitVectorCompressedColumn<T>::copy() const{
		return ColumnPtr(new BitVectorCompressedColumn<T>(*this));
	}

	template<class T>
	bool BitVectorCompressedColumn<T>::update(TID index, const boost::any& new_value){
		T value = boost::any_cast<T>(new_value);
		std::vector<bool> bit_vector;
		bool flag = false;

		for(unsigned int j = 0; j < val_vector.size(); j++)
		{
			if(val_vector[j].second[index] == true)	// find value in compressed vector, which should be changed 
			{
				val_vector[j].second[index] = false;
				// check if bit vector has another elements (true values) 
				std::vector<bool>::iterator it;
				it = std::find(val_vector[j].second.begin() , val_vector[j].second.end(), true);
				if (it == val_vector[j].second.end()) // if another element was not found 
				{
					val_vector.erase(val_vector.begin() + j);
				}
			}
		}
	
		for(unsigned int i = 0; i < val_vector.size(); i++)
		{
			if (val_vector[i].first == value) // if new value is equal to some value in compressed vector 
			{
				val_vector[i].second[index] = true;
				flag = true;
				break;
			}
		}
		if (flag == false) // if new value isn't equal to some value in compressed vector 
		{
			bit_vector.resize(val_vector[0].second.size(), false);
			bit_vector[index] = true;
			val_vector.push_back(std::make_pair(value, bit_vector));
		}
		return true;
	}

	template<class T>
	bool BitVectorCompressedColumn<T>::update(PositionListPtr , const boost::any& ){
		return false;		
	}
	
	template<class T>
	bool BitVectorCompressedColumn<T>::remove(TID index){
		for(unsigned int j = 0; j < val_vector.size(); j++)
		{
			if(val_vector[j].second[index] == true)	// find value in compressed vector, whose bit-vector should be changed
			{
				for(unsigned int i = 0; i < val_vector.size(); i++)
				{
					val_vector[i].second.erase(val_vector[i].second.begin() + index);
				}
				// check if bit vector has another elements (true values)
				std::vector<bool>::iterator it;
				it = std::find(val_vector[j].second.begin() , val_vector[j].second.end(), true);
				if (it == val_vector[j].second.end()) // if another element was not found 
				{
					val_vector.erase(val_vector.begin() + j);
				}
				return true;
			}
		}
		return false;	
	}
	
	template<class T>
	bool BitVectorCompressedColumn<T>::remove(PositionListPtr){
		return false;			
	}

	template<class T>
	bool BitVectorCompressedColumn<T>::clearContent(){
		val_vector.clear();
		return true;
	}

	template<class T>
	bool BitVectorCompressedColumn<T>::store(const std::string& path_ ){
//		string path("data/");
		std::string path(path_);
		path += "/";
		path += this->name_;
		//std::cout << "Writing Column " << this->getName() << " to File " << path << std::endl;
		std::ofstream outfile (path.c_str(),std::ios_base::binary | std::ios_base::out);
		boost::archive::binary_oarchive oa(outfile);

		oa << val_vector;

		outfile.flush();
		outfile.close();
			return true;
		//return false;
	}
	template<class T>
	bool BitVectorCompressedColumn<T>::load(const std::string& path_){
		std::string path(path_);
		//std::cout << "Loading column '" << this->name_ << "' from path '" << path << "'..." << std::endl;
		//string path("data/");
		path += "/";
		path += this->name_;
		
		//std::cout << "Opening File '" << path << "'..." << std::endl;
		std::ifstream infile (path.c_str(),std::ios_base::binary | std::ios_base::in);
		boost::archive::binary_iarchive ia(infile);

		ia >> val_vector;
		infile.close();
		return true;
	//	return false;
	}

	template<class T>
	T& BitVectorCompressedColumn<T>::operator[](const int index){
		static T t;
		for(unsigned int i = 0; i < val_vector.size(); i++)
		{
			if(val_vector[i].second[index] == true)
			{
				t = val_vector[i].first;
				break;
			}
		}
		return t;
	}

	template<class T>
	unsigned int BitVectorCompressedColumn<T>::getSizeinBytes() const throw(){
	return	val_vector.capacity() * (val_vector[0].second.capacity() * sizeof(bool) + sizeof(T));
		//return 0; //return values_.capacity()*sizeof(T);
	}

/***************** End of Implementation Section ******************/



}; //end namespace CogaDB

/*namespace boost
{
	namespace serialization 
	{
		template<class Archive, class T>
		void serialize(Archive & ar, std::pair<T, std::vector<bool>> & g, const unsigned int version)
		{
			ar & g.first;
			ar & g.second;
		}
	}
}; */

