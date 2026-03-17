# app.py

import streamlit as st
import ftplib
import io
import traceback
import pandas as pd # Import conservé pour compatibilité future

# --- CONFIGURATION ET NUMÉRO DE VERSION ---
APP_VERSION = "v1.10.2" # Correction bug variable 'display_name' dans Modifier
FTP_HOST = "ftp.figarocms.fr"
FTP_USER = "apimo-auto-fab"

# --- FONCTIONS TECHNIQUES FTP ---

def connect_ftp(host, user, password):
    try:
        ftp = ftplib.FTP_TLS(timeout=60)
        ftp.connect(host)
        
        # 1. On s'authentifie SANS forcer le chiffrement des données 
        # (secure=False évite l'erreur 504 "Command not implemented")
        ftp.login(user, password, secure=False)
        
        # 2. On active le mode passif
        ftp.set_pasv(True)
        
        # 3. LE CORRECTIF ANTI-CONNEXION REFUSÉE (Erreur 111) :
        # On force Python à utiliser la vraie adresse du serveur (ftp.figarocms.fr) 
        # au lieu de l'adresse interne que le serveur renvoie parfois par erreur.
        original_makepasv = ftp.makepasv
        def patched_makepasv():
            _, port = original_makepasv() # On récupère juste le port
            return host, port             # On injecte manuellement le bon "host"
        ftp.makepasv = patched_makepasv
        
        return ftp
    except Exception as e:
        st.error(f"La connexion FTP a échoué : {e}")
        return None

def check_id_for_site(ftp, agency_id, site):
    """
    Scanne les fichiers CSV sur le FTP pour trouver l'ID.
    Retourne : Liste de tuples (chemin_fichier, mode_contact)
    """
    if site == 'figaro':
        files_to_check = [("All", 'apimo_1.csv'), ("/", 'apimo_11.csv'), ("/", 'apimo_12.csv'), ("/", 'apimo_13.csv')]
    elif site == 'proprietes':
        files_to_check = [("All", 'apimo_3.csv'), ("/", 'apimo_31.csv'), ("/", 'apimo_32.csv'), ("/", 'apimo_33.csv')]
    else:
        return []
    
    agency_id_str = str(agency_id)
    found_results = []
    
    for path, filename in files_to_check:
        try:
            ftp.cwd("/")
            if path != "/": ftp.cwd(path)
            
            r = io.BytesIO()
            ftp.retrbinary(f'RETR {filename}', r.write)
            r.seek(0)
            
            content = r.getvalue().decode('utf-8', errors='ignore')
            for line in content.splitlines():
                if line.strip().startswith(agency_id_str + ','):
                    parts = line.strip().split(',')
                    contact_mode = parts[-1] if len(parts) >= 5 else '?'
                    
                    if path == "/": clean_path = f"/{filename}"
                    else: clean_path = f"{path}/{filename}"
                        
                    found_results.append((clean_path, contact_mode))
                    break 
        except Exception: pass
            
    return found_results

def check_coherence(results, site_name):
    """Affiche des alertes si la config FTP est incohérente"""
    if not results: return
    has_global = any("All/" in path for path, _ in results)
    has_split = any("All/" not in path for path, _ in results)
    
    if has_global and has_split:
        st.caption(f"✅ Configuration {site_name} cohérente (Présent Global + Split).")
    elif has_global and not has_split:
        st.error(f"⚠️ Configuration {site_name} INCOMPLÈTE : Présent dans Global mais manquant dans les fichiers scindés.")
    elif not has_global and has_split:
        st.error(f"⚠️ Configuration {site_name} INCOMPLÈTE : Présent dans un fichier scindé mais manquant dans Global.")

# --- FONCTIONS D'ACTION (CRUD) ---

def ajouter_client(ftp, agency_id, site, contact_mode, add_to_global=True, add_to_split=True):
    if site == 'figaro':
        login, global_file, prefix, indices = '694', 'apimo_1.csv', 'apimo_1', ['1', '2', '3']
    elif site == 'proprietes':
        login, global_file, prefix, indices = '421', 'apimo_3.csv', 'apimo_3', ['1', '2', '3']
    else:
        st.error("Site non valide."); return

    agency_id_str = str(agency_id)
    # Hash codé en dur comme demandé
    new_line_record = f"{agency_id_str},{login},df93c3658a012b239ff59ccee0536f592d0c54b7,agency,{contact_mode}"
    path_global, path_split = "All", "/"

    def append_content_robust(ftp_path, ftp_filename, new_record):
        ftp.cwd("/")
        if ftp_path != "/": ftp.cwd(ftp_path)
        lines = []
        try:
            content_in_memory = io.BytesIO()
            ftp.retrbinary(f'RETR {ftp_filename}', content_in_memory.write)
            content_decoded = content_in_memory.getvalue().decode('utf-8', errors='ignore')
            lines = [line for line in content_decoded.splitlines() if line.strip()]
        except ftplib.error_perm: pass
        lines.append(new_record)
        new_content = "\n".join(lines)
        content_to_upload = io.BytesIO(new_content.encode('utf-8'))
        ftp.cwd("/")
        if ftp_path != "/": ftp.cwd(ftp_path)
        ftp.storbinary(f'STOR {ftp_filename}', content_to_upload)
        st.info(f"Fichier mis à jour : {ftp_path}/{ftp_filename}")

    # 1. Ajout Global
    if add_to_global:
        st.write(f"Ajout au fichier Global ({global_file})...")
        append_content_robust(path_global, global_file, new_line_record)
    else:
        st.info(f"Le client est déjà présent dans le fichier Global ({global_file}). Ajout ignoré.")

    # 2. Ajout Split (Load Balancing)
    if add_to_split:
        st.write(f"Analyse des fichiers scindés ({prefix}...) pour le site '{site}'...")
        ftp.cwd(path_split)
        nlst = ftp.nlst()
        line_counts = {}
        already_exists_in_split = False
        found_in_file = ""
        
        # Scan préventif anti-doublon
        for i in indices:
            filename = f"{prefix}{i}.csv"
            if filename in nlst:
                content_in_memory = io.BytesIO()
                ftp.retrbinary(f'RETR {filename}', content_in_memory.write)
                content_str = content_in_memory.getvalue().decode('utf-8', errors='ignore')
                lines = [line for line in content_str.splitlines() if line.strip()]
                for line in lines:
                    if line.startswith(agency_id_str + ','):
                        already_exists_in_split = True
                        found_in_file = filename
                        break
                if already_exists_in_split: break
                line_counts[filename] = len(lines)
            else: line_counts[filename] = 0

        if already_exists_in_split:
            st.warning(f"⚠️ Action annulée pour les fichiers scindés : L'ID {agency_id} a été trouvé dans **{found_in_file}**.")
        elif line_counts:
            smallest_file = min(line_counts, key=line_counts.get)
            st.info(f"Le fichier le plus léger est : {smallest_file} ({line_counts[smallest_file]} lignes). Mise à jour...")
            append_content_robust(path_split, smallest_file, new_line_record)
        else: st.error("Impossible de trouver les fichiers scindés sur le serveur.")
    else: st.info("La logique a déterminé que l'ID est déjà présent dans un fichier scindé.")

def supprimer_client(ftp, agency_id, site):
    if site == 'figaro':
        files_to_check = [("All", 'apimo_1.csv'), ("/", 'apimo_11.csv'), ("/", 'apimo_12.csv'), ("/", 'apimo_13.csv')]
    elif site == 'proprietes':
        files_to_check = [("All", 'apimo_3.csv'), ("/", 'apimo_31.csv'), ("/", 'apimo_32.csv'), ("/", 'apimo_33.csv')]
    else: st.error(f"Site '{site}' non valide pour la suppression."); return

    agency_id_str, found = str(agency_id), False
    for path, filename in files_to_check:
        try:
            ftp.cwd("/")
            if path != "/": ftp.cwd(path)
            r = io.BytesIO()
            ftp.retrbinary(f'RETR {filename}', r.write)
            r.seek(0)
            if r.getbuffer().nbytes == 0: continue
            lines = [line.strip() for line in r.getvalue().decode('utf-8', errors='ignore').splitlines() if line.strip()]
            initial_rows = len(lines)
            lines_filtered = [line for line in lines if not line.startswith(agency_id_str + ',')]
            if len(lines_filtered) < initial_rows:
                found = True
                new_content = "\n".join(lines_filtered)
                content_io = io.BytesIO(new_content.encode('utf-8'))
                ftp.cwd("/")
                if path != "/": ftp.cwd(path)
                ftp.storbinary(f'STOR {filename}', content_io)
                st.info(f"ID {agency_id_str} supprimé dans {path}/{filename}")
        except Exception: pass
    if not found: st.warning(f"L'ID d'agence {agency_id_str} n'a été trouvé dans aucun fichier du site '{site}'.")

def modifier_client(ftp, agency_id, site, new_contact_mode):
    if site == 'figaro':
        files_to_check = [("All", 'apimo_1.csv'), ("/", 'apimo_11.csv'), ("/", 'apimo_12.csv'), ("/", 'apimo_13.csv')]
    elif site == 'proprietes':
        files_to_check = [("All", 'apimo_3.csv'), ("/", 'apimo_31.csv'), ("/", 'apimo_32.csv'), ("/", 'apimo_33.csv')]
    else: st.error(f"Site '{site}' non valide pour la modification."); return
    agency_id_str, found_and_modified = str(agency_id), False
    for path, filename in files_to_check:
        try:
            ftp.cwd("/")
            if path != "/": ftp.cwd(path)
            r = io.BytesIO()
            ftp.retrbinary(f'RETR {filename}', r.write)
            r.seek(0)
            if r.getbuffer().nbytes == 0: continue
            lines = [line.strip() for line in r.getvalue().decode('utf-8', errors='ignore').splitlines() if line.strip()]
            new_lines = []
            file_was_modified = False
            for line in lines:
                if line.startswith(agency_id_str + ','):
                    parts = line.split(',')
                    if len(parts) >= 5:
                        new_line = f"{parts[0]},{parts[1]},{parts[2]},{parts[3]},{new_contact_mode}"
                        new_lines.append(new_line)
                        file_was_modified = True
                        found_and_modified = True
                    else: new_lines.append(line)
                else: new_lines.append(line)
            if file_was_modified:
                new_content = "\n".join(new_lines)
                content_io = io.BytesIO(new_content.encode('utf-8'))
                ftp.cwd("/")
                if path != "/": ftp.cwd(path)
                ftp.storbinary(f'STOR {filename}', content_io)
                st.info(f"ID {agency_id_str} modifié dans {path}/{filename}")
        except Exception: pass
    if not found_and_modified: st.warning(f"L'ID d'agence {agency_id_str} n'a pas été trouvé pour modification dans les fichiers du site '{site}'.")

def verifier_parametrage_ftp(ftp, agency_id, site_choice):
    """
    Vérification standard sur le FTP.
    """
    st.info(f"Recherche de l'ID d'agence '{agency_id}' sur le FTP...")
    
    results_figaro = check_id_for_site(ftp, agency_id, 'figaro')
    results_proprietes = check_id_for_site(ftp, agency_id, 'proprietes')
    all_results = results_figaro + results_proprietes
    
    if all_results:
        st.success(f"L'ID d'agence '{agency_id}' est présent :")
        for file_path, mode in all_results:
            mode_text = "Email Agence (0)" if mode == '0' else "Email Négociateur (1)" if mode == '1' else f"Valeur inconnue ({mode})"
            st.write(f"- Dans **{file_path}** avec le mode : **{mode_text}**")
        
        # Vérification de cohérence
        if site_choice == 'Figaro Immobilier' or site_choice == 'Les deux':
            check_coherence(results_figaro, "Figaro Immobilier")
        if site_choice == 'Propriétés Le Figaro' or site_choice == 'Les deux':
            check_coherence(results_proprietes, "Propriétés Le Figaro")
    else:
        st.info(f"L'ID d'agence '{agency_id}' n'a été trouvé dans aucun fichier CSV.")


# --- INTERFACE UTILISATEUR ---
st.title("Outil de gestion des flux Apimo")

col1, col2 = st.columns(2)

with col1:
    action = st.radio("Action :", ('Ajouter', 'Supprimer', 'Vérifier', 'Modifier le mode de contact'))
    agency_id_input = st.text_input("Agency ID :")
    ftp_password = st.text_input("Mot de passe FTP :", type="password", help="Requis pour accéder aux fichiers.")

with col2:
    site_choice = st.radio("Site(s) :", ('Figaro Immobilier', 'Propriétés Le Figaro', 'Les deux'))
    contact_mode_options = {'Email Agence (0)': 0, 'Email Négociateur (1)': 1}
    
    # Le mode de contact ne sert que pour l'ajout/modif
    if action == 'Ajouter' or action == 'Modifier le mode de contact':
        contact_mode = st.selectbox("Mode de contact :", options=list(contact_mode_options.keys()))
    else:
        contact_mode = None

# --- EXÉCUTION ---
if st.button("Exécuter"):
    agency_id = agency_id_input.strip()
    if not agency_id:
        st.error("L'Agency ID est obligatoire.")
    elif not ftp_password:
        st.error("Le mot de passe FTP est obligatoire.")
    else:
        ftp = None
        try:
            with st.spinner("Connexion au serveur FTP..."):
                ftp = connect_ftp(FTP_HOST, FTP_USER, ftp_password)
            if ftp:
                st.success("Connexion FTP réussie.")
                
                site_display_names = {'figaro': 'Figaro Immobilier', 'proprietes': 'Propriétés Le Figaro'}
                sites_to_process = []
                if site_choice == 'Figaro Immobilier': sites_to_process.append('figaro')
                elif site_choice == 'Propriétés Le Figaro': sites_to_process.append('proprietes')
                elif site_choice == 'Les deux': sites_to_process.extend(['figaro', 'proprietes'])
                
                with st.spinner(f"Opération '{action}' en cours..."):
                    
                    if action == 'Vérifier':
                        verifier_parametrage_ftp(ftp, agency_id, site_choice)

                    elif action == 'Ajouter':
                        for site_code in sites_to_process:
                            display_name = site_display_names.get(site_code, site_code.upper())
                            st.subheader(f"Traitement : {display_name}")
                            existing = check_id_for_site(ftp, agency_id, site_code)
                            in_global = any("All/" in r[0] for r in existing)
                            in_split = any("All/" not in r[0] for r in existing)
                            if in_global and in_split:
                                st.warning(f"ID {agency_id} déjà configuré pour {display_name}.")
                                continue
                            ajouter_client(ftp, agency_id, site_code, contact_mode_options[contact_mode], not in_global, not in_split)

                    elif action == 'Supprimer':
                        for site_code in sites_to_process:
                            display_name = site_display_names.get(site_code, site_code.upper())
                            st.subheader(f"Suppression : {display_name}")
                            supprimer_client(ftp, agency_id, site_code)

                    elif action == 'Modifier le mode de contact':
                        for site_code in sites_to_process:
                            # CORRECTION ICI : On définit display_name avant de l'utiliser
                            display_name = site_display_names.get(site_code, site_code.upper())
                            st.subheader(f"Modification : {display_name}")
                            modifier_client(ftp, agency_id, site_code, contact_mode_options[contact_mode])
                        
                st.success("Opération terminée.")
        except Exception:
            st.error("Une erreur inattendue est survenue.")
            st.code(traceback.format_exc())
        finally:
            if ftp:
                try: ftp.quit()
                except: pass

st.markdown(f"<div style='text-align: center; color: grey; font-size: 0.8em;'>Version {APP_VERSION}</div>", unsafe_allow_html=True)