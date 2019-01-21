#!/bin/bash -e
## @file offline_fesom_to_ocean.sh
## @brief
## Makes `PICO` input from `FESOM` output!
##
## This script sets up a SLURM job which is able to
## generate a `PISM` capable input for the `PICO` ocean
## component from a `FESOM` output
##
## Elements of the script can be used seperately, you
## just need to source the file and call the functions
## that you might need.
##
## @author Dr. Paul Gierz
## @author AWI Bremerhaven

#Xsrun  I know what I am doing

################################################################################
################################################################################
################################################################################
#
#               F U N C T I O N     D E F I N I T I O N S
#
################################################################################
################################################################################
################################################################################

## @fn get_config()
## @brief
## Reads configuration from a file
get_config() {
        config_file=${1}
        env_current=$(env | sort)
        source ${config_file}
        env_new=$(env | sort)
        comm -13 <(echo "${env_current}") <(echo "${env_new}")
}

## @fn extract_fesom_data()
## @brief
## Gets relevant `FESOM` data from a experiment folder
##
## Gets the `FESOM` output and combines it to a common file.
## We assume that there are variables set:
## 1. FESOM_OUTPUT_DIR (required)
## 2. TEMP_VARNAME (required if ESM_STYLE is not yes)
## 3. SALT_VARNAME (required if ESM_STYLE is not yes)
## 4. ESM_STYLE (default "yes")
extract_fesom_data() {
        if [ "x${ESM_STYLE}" == "xyes" ]; then
                cdo merge \
                        "${FESOM_OUTPUT_DIR}"/thetao.fesom.????.nc \
                        "${FESOM_OUTPUT_DIR}"/so.fesom.????.nc \
                        fesom_temperature_salinity.nc
        fi

        if [ "x${ESM_STYLE}" == "xno" ]; then
                cdo merge \
                        "${FESOM_OUTPUT_DIR}"/"${TEMP_VARNAME}".fesom.????.nc \
                        "${FESOM_OUTPUT_DIR}"/"${SALT_VARNAME}".fesom.????.nc \
                        fesom_temperature_salinity.nc
        fi
        export FESOM_FILE=fesom_temperature_salinity.nc
        cleanup_list="fesom_temperature_salinity.nc ${cleanup_list}"
}

## @fn get_ten_years()
## @brief
## extracts ten year chunks
##
## Arguments:
##      * $1: start_year -- beginning of the ten year chunk you want to extract
get_ten_years() {
        start_year=${1}
        end_year=$((start_year + 10))
        if [ ! -f "${FESOM_FILE%.*}"_"${start_year}"-"${end_year}".nc ]; then
                cdo seltimestep,"${start_year}"/"${end_year}" \
                        "${FESOM_FILE}" \
                        "${FESOM_FILE%.*}"_"${start_year}"-"${end_year}".nc
        fi
        export FESOM_CHUNK_FILE="${FESOM_FILE%.*}"_"${start_year}"-"${end_year}".nc
        cleanup_list="${FESOM_CHUNK_FILE} ${cleanup_list}"
}

## @fn round_up()
## @brief
## Rounds up to the nearest 10, 100, 1000
##
## function to round up to 1- the nearest integer on a given scale (10, 100, 1000 are allowed scales)
## the result is given back using echo, and can be derived by means of command substitution (e.g. result=$(round_up 2871 100))
##
## @author Dr. Christian Stepanek
round_up() {
        #echo "value of scale: ${2}" #debug CS
        #echo "value of number: ${1}" #debug CS
        local scale=${2}
        local number=${1}
        if [ ${scale} != 10 ] && [ ${scale} != 50 ] && [ ${scale} != 100 ] &&  [ ${scale} != 500 ] && [ ${scale} != 1000 ]; then
        #given scale is not valid; returning initial value!
                echo
                echo '!!!ERROR!!!'
                echo "given block size ${scale} is not valid; using inital value ${number} (i.e. using scale of 1) ..."
                echo '!!!ERROR!!!'
                echo ${number}
                return
        else
                remainder=$((${number} % ${scale}))
                result=$((${number}+(${scale}-${remainder})-1))
                echo ${result}
        fi
}


## @fn fesom_scalar_to_LatLon()
## @brief
## Runs the python program fesom_scalar_to_LatLon
##
## Arguments:
##      * $1: FESOM_FILE
##      * $2: FESOM_VARIABLE
##      * $3: FESOM_MESH (defaults to CORE2)
##      * $4: FESOM_OUTPUT (defaults to a modifed version of ${FESOM_FILE}
fesom_scalar_to_LatLon() {
        FESOM_CHUNK_FILE=${1}
        FESOM_VARIABLE=${2}
        FESOM_MESH=${3}
        FESOM_OUTPUT=${4}

        if [ "x${FESOM_MESH}" == "x" ]; then
                export FESOM_MESH=/work/ollie/pool/FESOM/meshes_default/core
        fi

        if [ "x${FESOM_OUTPUT}" == "x" ]; then
                export FESOM_OUTPUT="${FESOM_CHUNK_FILE%.*}_latlon.nc"
        fi

        if [ -f ${FESOM_OUTPUT} ]; then
                return
        fi

        python fesom_scalar_array_to_LonLat.py \
                --FESOM_FILE "${FESOM_CHUNK_FILE}" \
                --FESOM_VARIABLE "${FESOM_VARIABLE}" \
                --FESOM_MESH "${FESOM_MESH}" \
                --FESOM_OUTPUT " ${FESOM_OUTPUT}"
}

check_allowed_cdo_remap_flag() {
    # Check allowed remapping type flags in the frame work
    # of the here used script environment
    # Call: check_allowed_cdo_remap_flag RemapType
    # Christian Rodehacke, 2018-09-05
    __remap_type=$1
    case ${__remap_type} in
	bil)
	    echo "                -   Remapping ${__remap_type} : bilinear (for curvelinear grid)"
	    ;;
	bic)
	    echo "                -   Remapping ${__remap_type} : bicubic (for curvelinear grid)"
	    ;;
	nn)
	    echo "                -   Remapping ${__remap_type} : nearest neighbor (any grid)"
	    ;;
	dis)
	    echo "                -   Remapping ${__remap_type} : distance-weighted average (any grid)"
	    ;;
	con)
	    echo "                -   Remapping ${__remap_type} : First order conservative (requires corner points)"
	    ;;
	ycon)
	    echo "                -   Remapping ${__remap_type} : First order conservative, YAC (requires corner points)"
	    ;;
	con2)
	    echo "                -   Remapping ${__remap_type} : Second order conservative (requires corner points)"
	    ;;
	laf)
	    echo "                -   Remapping ${__remap_type} : largest area fraction (for spherical grid)"
	    ;;
	*)
	    echo " UNKNOWN remapping <<${__remap_type}>>"
	    echo "   Known: bil, bic, nn, dis, con, ycon, con2, laf"
	    echo " S T O P  1"
	    exit 1
	    ;;
    esac
    unset __remap_type
}

build_weights4remap() {
    # Build remapping weights (cdo griddes) for provided input 
    # or use existing restart weights of it exists
    # Call: build_grid_des InputData, GridDesFileName WeightFileName RemapType (TargetDir) (RestartDir) (VariableSelection)
    # Christian Rodehacke, 2018-09-05
    _source_file=$1
    _griddes_file=$2
    _weight_file=$3
    _remap_type=$4
    _target_dir=$5
    _restart_dir=$6
    _selvar2regrid=$7

    _restart_weight_file=$_weight_file

    if [ -f $_restart_weight_file ] ; then
	echo "                -   Reuse restart weight file $_restart_weight_file"
	use_weight_file=$_restart_weight_file
    else
	echo "                -   Compute new weight file >>${_weight_file}<< based on >>${_source_file}<<"

	check_allowed_cdo_remap_flag ${_remap_type}

	if [ "x${vars2regrid}" == "x" ] ; then
	    cdo -s gen${_remap_type},${_griddes_file} \
		-seltimestep,1 $_source_file \
		$_weight_file
	else
	    cdo -s gen${_remap_type},${_griddes_file} \
		-selvar,${_selvar2regrid} -seltimestep,1 $_source_file \
		$_weight_file
	fi
	use_weight_file=$(pwd)/$_weight_file

    fi

    unset _griddes_file _restart_dir _remap_type _restart_weight_file
    unset _selvar2regrid _source_file _target_dir use_weight_file _weight_file
}

## @fn fesom_scalar_LatLon_to_PISM_Grid()
## @brief
## Interpolates a `FESOM` file with already attached Lat/Lon information
## to a `PISM` grid
##
## Arguments
fesom_scalar_LatLon_to_PISM_Grid() {
        build_weights4remap \
                ${FESOM_OUTPUT} \
                ${GRIDDES_ICE} \
                weights_oce2ice.dis.nc \
                dis
        cdo remap,${GRIDDES_ICE},weights_oce2ice.dis.nc \
                ${FESOM_OUTPUT} \
                ${FESOM_OUTPUT%.*}.dis.nc
        cdo fillmiss2 ${FESOM_OUTPUT%.*}.dis.nc ${FESOM_OUTPUT%.*}_tmp
        mv ${FESOM_OUTPUT%.*}_tmp ${FESOM_OUTPUT%.*}.dis.nc
}

## @fn runme()
## @brief
## Submits this script to the sbatch queue, reading the configuration from
## offline_fesom_to_ocean.config
##
## Since we do not necessarily know how big the array needs to be that we
## want to submit, a small amount of preprocesing needs to happen on the 
## login node. Then, we submit the array job.
runme() {
        get_config ${1}

        if [ "x${FESOM_FILE}" == "x" ]; then
                extract_fesom_data
        fi

        if [[ $(hostname) == *"ollie"* ]]; then
                array_size=$(cdo ntime $FESOM_FILE)
                if [ $(echo ${array_size} | wc -c) -eq 2 ]; then
                        scale=10
                elif [ $(echo ${array_size} | wc -c) -eq 3 ]; then
                        scale=100
                elif [ $(echo ${array_size} | wc -c) -eq 4 ]; then
                        scale=1000
                fi
                array_size=$(round_up ${array_size} ${scale})

                sbatch \
                        --partition=fat,xfat \
                        --cpus-per-task=1 \
                        --no-requeue \
                        --time=00:30:00 \
                        --output=offline_fesom_to_pico-%A-%a.out \
                        --array=1-${array_size}:10  "${0}" "${1}"
        elif [[ $(hostname) == *"prod"* ]] || [[ $(hostname) == *"mini"* ]] || [[ $(hostname) == *"fat"* ]]; then
                echo "Running on $(hostname)"
                echo "Running in $(pwd)"
                echo "Starting..."
                date
                get_ten_years ${SLURM_ARRAY_TASK_ID}
                fesom_scalar_to_LatLon ${FESOM_CHUNK_FILE} ${FESOM_VARIABLE} ${FESOM_MESH} ${FESOM_OUTPUT}
                fesom_scalar_LatLon_to_PISM_Grid ${FESOM_OUTPUT} ${PISM_GRID} ${PISM_INPUT}
                #PISM_PICO_set_names ${PISM_INPUT}
                rm ${cleanup_list}
                echo "...done!"
                date
        else
                echo "This script only works on ollie!"
                echo "I thought you were on: $(hostname)"
                echo "Goodbye"
                exit
        fi

}

################################################################################
################################################################################
################################################################################
#
#               E X E C U T I O N
#
################################################################################
################################################################################
################################################################################
module purge
module load python
module load cdo
module load centoslibs
module list
runme ${1}
