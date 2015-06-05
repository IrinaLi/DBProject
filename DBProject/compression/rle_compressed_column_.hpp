
/*! 
 * 
 */

#pragma once

#include <core/compressed_column.hpp>

namespace CoGaDB{
	

/*!
 *  \brief     This class represents a column with type T, is the class for RLE-compression.
 */	
template<class T>
class RLECompressedColumn : public CompressedColumn<T>{
	public:
	/***************** constructors and destructor *****************/
	RLECompressedColumn(const std::string& name, AttributeType db_type);
	virtual ~RLECompressedColumn();

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

	std::vector< std::pair<int,T> > val_vector;

};


/***************** Start of Implementation Section ******************/

	
	template<class T>
	RLECompressedColumn<T>::RLECompressedColumn(const std::string& name, AttributeType db_type) : CompressedColumn<T>(name, db_type){

	}

	template<class T>
	RLECompressedColumn<T>::~RLECompressedColumn(){

	}

	template<class T>
	bool RLECompressedColumn<T>::insert(const boost::any&){

		return false;
	}

	template<class T>
	bool RLECompressedColumn<T>::insert(const T& new_value){

		if(val_vector.empty()) 
		{
			val_vector.push_back(std::make_pair(1, new_value));
		}
		else
		{
			if(val_vector.back().second == new_value)
			{
				val_vector.back().first++;
			}
			else
			{
				val_vector.push_back(std::make_pair(1, new_value));
			}
		}
		return true;
	}

	template <typename T> 
	template <typename InputIterator>
	bool RLECompressedColumn<T>::insert(InputIterator , InputIterator ){
		
		return true;
	}

	template<class T>
	const boost::any RLECompressedColumn<T>::get(TID){

		return boost::any();
	}

	template<class T>
	void RLECompressedColumn<T>::print() const throw(){

	}
	template<class T>
	size_t RLECompressedColumn<T>::size() const throw(){
		size_t n = 0;
		for(unsigned int i = 0; i < val_vector.size(); i++)
		{ 
			 n += val_vector[i].first;
		}

		return n;
	}
	template<class T>
	const ColumnPtr RLECompressedColumn<T>::copy() const{

		return ColumnPtr();
	}

	template<class T>
	bool RLECompressedColumn<T>::update(TID index, const boost::any& new_value ){
		
		T value = boost::any_cast<T>(new_value);
		unsigned int start_index = 0;
		unsigned int end_index = val_vector[0].first-1;
		for(unsigned int i = 0; i < val_vector.size(); i++)
		{  
			if(index >= start_index && index <= end_index)
			{
				// check if current and new value are the same
				if (val_vector[i].second == value)             
				{
					break;
				}
				// check if compressed element contains one value
				if(index == start_index && index == end_index)
				{
					// check that compressed column doesn't end or start with current element  
					if(i != 0 && i != val_vector.size() - 1) 
					{
						//if new value is equal to previous and next element, merge three elements into one
						if (val_vector[i-1].second == value && val_vector[i+1].second == value)
						{
							val_vector[i-1].first += val_vector[i+1].first + 1;
							val_vector.erase(val_vector.begin() + i);
							val_vector.erase(val_vector.begin() + (i+1));
							break;
						}
					}
				}
				if (index == start_index)
				{
					//check that compressed column doesn't start with current compressed element, 
					//and that new value is equal to value of compressed element 
					if(i != 0 && val_vector[i-1].second == value)
					{
						val_vector[i-1].first++;
						if (val_vector[i].first > 1)
						{
							val_vector[i].first--;
						}
						else 
						{
							val_vector.erase(val_vector.begin() + i);
						}
					}
					else 
					{
						if (val_vector[i].first == 1)
						{
							val_vector[i].second = value;
						}
						else 
						{
							val_vector.insert(val_vector.begin() + i, std::make_pair(1, value));
							val_vector[i+1].first--;
						}
					}
					break;
				}
				if (index == end_index)
				{
					if(i != val_vector.size()-1 && val_vector[i+1].second == value)
					{
						val_vector[i+1].first++;
						if(val_vector[i].first > 1)
						{
							val_vector[i].first--;
						}
						else 
						{								
							val_vector.erase(val_vector.begin() + i);
						}
					}
					else 
					{

						/*if (val_vector[i].first == 1)
						{
							val_vector[i].second = value;
						}
						else
						{
							val_vector.insert(val_vector.begin()+i+1, std::make_pair(1, value));
							val_vector[i].first--;
						}*/
					}
					break;
				}
				val_vector[i].first = index - start_index;
				val_vector.insert(val_vector.begin()+i+1, std::make_pair(1, value));
				val_vector.insert(val_vector.begin()+i+2, std::make_pair((end_index - index), val_vector[i].second));
				break;
			}
			else
			{
				start_index += val_vector[i].first;
				end_index += val_vector[i+1].first;
			}
		}
		return true;
	}

	template<class T>
	bool RLECompressedColumn<T>::update(PositionListPtr , const boost::any& ){
		return false;		
	}
	
	template<class T>
	bool RLECompressedColumn<T>::remove(TID){
		return false;	
	}
	
	template<class T>
	bool RLECompressedColumn<T>::remove(PositionListPtr){
		return false;			
	}

	template<class T>
	bool RLECompressedColumn<T>::clearContent(){
		return false;
	}

	template<class T>
	bool RLECompressedColumn<T>::store(const std::string&){
		return false;
	}
	template<class T>
	bool RLECompressedColumn<T>::load(const std::string&){
		return false;
	}

	template<class T>
	T& RLECompressedColumn<T>::operator[](const int index){
		
		static T t;
		int k = val_vector[0].first;
		for(unsigned int i = 0; i < val_vector.size(); i++)
		{  
			if(k >= index+1)
			{
				return val_vector[i].second;
			}
			else
			{
				k += val_vector[i].first;
			}
		}
		return t;
	}

	template<class T>
	unsigned int RLECompressedColumn<T>::getSizeinBytes() const throw(){
		return 0; //return values_.capacity()*sizeof(T);
	}

/***************** End of Implementation Section ******************/



}; //end namespace CogaDB

